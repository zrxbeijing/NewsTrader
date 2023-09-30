import time
from urllib.parse import urlparse
import pandas as pd
import re
from transformers import pipeline
import requests
from sentence_transformers import SentenceTransformer
import scipy.spatial
import math
import enchant
import gdelt
from functools import partial
from NewsTrader.utils.accelerator import run_multitasking
import json
import os.path
from tqdm import tqdm
from warcio.archiveiterator import ArchiveIterator
from bs4.dammit import EncodingDetector


def extract_title(target_list, mode: str = 'info', check_english: bool = True, num_process: int = 1):
    """
    Extract the title info from the column Extras for gkg table. There is already some useful information in gkg table.
    We do not need to send queries to the original websites again if we just need some simple elements such as title.
    :param target_list: where to extract title from
    :param mode: how to extract title
    :param check_english: check whether words are english words
    :param num_process: number of processes
    :return: an augmented DataFrame with one more column 'title'
    """
    if mode == 'info':
        partial_func = partial(_extract_title_from_info)
        result_list = run_multitasking(
            partial_func, argument_list=target_list, num_workers=num_process, thread_or_process='process')

    elif mode == 'url':
        partial_func = partial(_extract_title_from_url, check_english=check_english)
        result_list = run_multitasking(
            partial_func, argument_list=target_list, num_workers=num_process, thread_or_process='process')

    return result_list


def _extract_title_from_url(url, check_english):
    """
    Extract title from given url.
    :param url: article url
    :param check_english: whether to check language
    :return: article title
    """
    title = None
    path = urlparse(url).path
    path_elements = path.split('/')
    path_elements = [element for element in path_elements if '_' in element or '-' in element]
    if len(path_elements) != 0:
        elements = [element.replace('_', ' ').replace('-', ' ') for element in path_elements]
        possible_titles = []
        for element in elements:
            word_list = element.split()
            if check_english:
                detector = enchant.Dict("en_US")
                possible_title = " ".join([word for word in word_list if detector.check(word)])
            else:
                possible_title = " ".join([word for word in word_list])
            possible_titles.append(possible_title)
        title_len = [len(title.split()) for title in possible_titles]
        # choose the longest one as title
        title_index = title_len.index(max(title_len))
        title = possible_titles[title_index]
    return title


def _extract_title_from_info(extra):
    """
    Extract title from the info column of the gkg table
    :param extra: the info column in a gkg table
    :return: article title
    """
    pattern = r'<PAGE_TITLE>(.*?)</PAGE_TITLE>'
    title = None
    if not pd.isna(extra):
        titles = re.findall(pattern, extra)
        if len(titles) != 0:
            title = " ".join(titles[0].strip().split())
    return title


def extract_symbols(titles,
                    model_name='bert-base-nli-mean-tokens',
                    min_score=0.6,
                    min_similarity=0.80,
                    num_workers=16,
                    preferred_exchange: list = ['NYS', 'NYQ', 'NAS', 'NMS', 'LSE', 'GER', 'FRA', 'JPX'],
                    preferred_country: list = ['United States', 'United Kingdom', 'Canada', 'Germany', 'France', 'Japan'],
                    only_preferred: bool = False):
    
    outputs_list = extrac_ner(titles, min_score)

    outputs_list = _prepare_matching(outputs_list=outputs_list, 
                                     model_name=model_name,
                                     num_workers=num_workers)
    
    result = _match_symbol(outputs_list=outputs_list, 
                           min_similarity=min_similarity,
                           preferred_exchange=preferred_exchange,
                           preferred_country=preferred_country,
                           only_preferred=only_preferred)

    return result


def extrac_ner(titles, min_score):
    ner = pipeline("ner", grouped_entities=True, device=0)
    print("Identifying entities...")
    outputs_list = ner(titles)
    outputs_list = [[output for output in outputs if (output['entity_group'] == 'I-ORG' or output['entity_group']=='ORG') and output['score'] >= min_score]
                    for outputs in outputs_list]
    return outputs_list


def _prepare_matching(outputs_list, model_name='bert-base-nli-mean-tokens', num_workers=16):
    """
    Add the following to the outputs_list returned by NER
    """
    # add embeddings to outputs_list
    outputs_list = _add_embedding(outputs_list, model_name=model_name)

    # add stock info
    outputs_list = _add_stock_info(outputs_list, num_workers=num_workers)

    # match symbol
    return outputs_list

    

def _add_embedding(outputs_list, model_name='bert-base-nli-mean-tokens'):    
    # add embeddings -> the length of this list is different from that of outputs_list
    embedder = SentenceTransformer(model_name)
    entity_words_list = [entity['word'] for outputs in outputs_list if len(outputs)!=0 for entity in outputs]
    embeddings_list = embedder.encode(entity_words_list)
    index_count = 0
    for outputs in outputs_list:
        if len(outputs) != 0:
            for output in outputs:
                output['embedding'] = embeddings_list[index_count]
                index_count += 1
    return outputs_list


def _add_stock_info(outputs_list, num_workers):
    # this is cpu computing intensive -> reduce time
    stock_df = get_stock_info()
    stock_df = stock_df.dropna(subset=['short_name']).reset_index(drop=True)
    stock_df = stock_df[['short_name', 'country', 'exchange', 'symbol', 'embedding']]
    stock_names = list(stock_df['short_name'])
    
    entity_words_list = [entity['word'] for outputs in outputs_list if len(outputs)!=0 for entity in outputs]

    partial_func = partial(_find_related_stock_index, stock_names= stock_names)
    target_indexes_list = run_multitasking(
        func=partial_func, argument_list=entity_words_list, num_workers=num_workers, thread_or_process='process')
    index_count = 0
    for outputs in outputs_list:
        if len(outputs) != 0:
            for output in outputs:
                output['stock_info'] = stock_df.loc[target_indexes_list[index_count]]
                index_count += 1

    return outputs_list


def _find_related_stock_index(word, stock_names):
    original_word = word.lower()
    company_names = [name.lower() for name in stock_names]
    targets_indexes = [i for i in range(len(company_names)) if original_word in company_names[i] ]

    return targets_indexes


def _match_symbol(outputs_list, 
                 min_similarity, 
                 preferred_exchange, 
                 preferred_country, 
                 only_preferred,
                 num_workers=16):

    print("Start matching...")

    partial_func = partial(_extract_symbol,
                           min_similarity=min_similarity,
                           preferred_exchange=preferred_exchange,
                           preferred_country=preferred_country,
                           only_preferred=only_preferred)

    result = run_multitasking(
        func=partial_func, argument_list=outputs_list, num_workers=num_workers, thread_or_process='process')
    return result


def _extract_symbol(outputs,
                    min_similarity=0.85,
                    preferred_exchange: list = ['NYS', 'NYQ', 'NAS', 'NMS', 'LSE', 'JPX', 'FRA', 'GER'],
                    preferred_country: list = ['United States', 'United Kingdom', 'Canada', 'Japan', 'Germany', 'France', 'China'],
                    only_preferred: bool = False):
    if len(outputs) == 0:
        return None

    query_results = []
    for output in outputs:
        stock_info = output['stock_info']
        if len(stock_info) == 0:
            continue
        corpus_embeddings = list(stock_info['embedding'])
        query_embedding = output['embedding']
        distances = scipy.spatial.distance.cdist([query_embedding], corpus_embeddings, "cosine")[0]
        scores = [1 - distance for distance in distances]
        sorted_index = sorted(range(len(scores)), key=lambda k: scores[k])
        sorted_index = [index for index in sorted_index if scores[index] >= min_similarity]
        if len(sorted_index) != 0:
            corresponding_results = [stock_info.iloc[k] for k in sorted_index]
            # select preferred/ideal result
            preferred_exchange_results = []
            for exchange in preferred_exchange:
                temporary_exchange_results = [result for result in corresponding_results if result['exchange'] == exchange]
                preferred_exchange_results += temporary_exchange_results
            
            preferred_results = []
            for country in preferred_country:
                temporary_country_results = [result for result in preferred_exchange_results if result['country'] == country]
                preferred_results += temporary_country_results

            if len(preferred_results) != 0:
                query_result = preferred_results[0]
            else:
                # if double criteria lead to None result, go back to the first criterium
                if len(preferred_exchange_results) != 0:
                    query_result = preferred_exchange_results[0]
                else:
                    if not only_preferred:
                        # if no preferred results, choose the one with highest similarity
                        query_result = corresponding_results[-1]
                    else:
                        continue
            query_results.append(query_result)
            query_results = [result[['short_name', 'symbol']] for result in query_results]
    if len(query_results) == 0:
        return None
    return query_results


def get_embedding(sentences, model_name_or_path: str = 'bert-base-nli-mean-tokens'):
    """
    :param sentences:
    :param model_name_or_path: which model to be applied
    """
    embedder = SentenceTransformer(model_name_or_path)
    embeddings = embedder.encode(sentences, batch_size=32, show_progress_bar=True)
    return list(embeddings)


def get_stock_info(base_url='https://raw.githubusercontent.com/JerBouma/FinanceDatabase/main/Database/Equities',
                   data_dir='.', 
                   file_name='stock_info.json'):
    file_path = os.path.join(data_dir, file_name)
    if os.path.exists(file_path):
        stock_df = pd.read_json(file_path)
        return stock_df
    # get data from github
    try:
        json_data = {}

        json_file_part_one = f"{base_url}/Equities Part 1.json"
        request = requests.get(json_file_part_one)
        json_data_segment = json.loads(request.text)
        json_data.update(json_data_segment)

        json_file_part_two = f"{base_url}/Equities Part 2.json"
        request = requests.get(json_file_part_two)
        json_data_segment = json.loads(request.text)
        json_data.update(json_data_segment)
    except json.decoder.JSONDecodeError:
        raise ValueError("Not able to find any data.")
    
    stock_df = pd.DataFrame(json_data).transpose()
    stock_df['symbol'] = stock_df.index
    stock_df = stock_df.dropna(subset=['symbol'])
    stock_df = stock_df.dropna(subset=['short_name'])
    stock_df = stock_df.reset_index(drop=True)
    stock_df.to_json(file_path)
    return stock_df

