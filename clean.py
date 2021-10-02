from sentence_transformers import SentenceTransformer
import scipy.spatial
import pandas as pd
from datetime import timedelta
from multiprocessing import cpu_count, Pool
from .accelerator import run_multitasking
from functools import partial


def clean_df(df, target_column):
    """
    Clean and prepare a pandas DataFrame. Three steps: drop duplicates, drop null value and reset index.
    :param df: original DataFrame
    :param target_column: the target column to be cleaned.
    :return: a prepared DataFrame
    """
    # remove duplicates
    df.drop_duplicates(subset=[target_column], inplace=True)
    # remove none
    df.dropna(subset=[target_column], inplace=True)
    # remove not text
    indicators = [
        True if type(row[target_column]) == str else False
        for index, row in df.iterrows()
    ]
    df = df.loc[indicators, :]
    # remove unnecessary strings
    df[target_column] = df[target_column].apply(lambda x: " ".join(x.split()))
    # remove empty string
    df = df.loc[df[target_column] != "", :]
    df.reset_index(drop=True, inplace=True)
    return df


def drop_duplicate_news(
    df,
    ticker_column: str = "ticker",
    date_column: str = "Date",
    embedding_column: str = "embeddings",
    look_back: int = 7,
    min_similarity: float = 0.8,
):
    """
    Apply sentence-bert to find duplicate news by checking the cosine similarity of news titles.
    This may take some time as it applies sentence-bert model to do inference.
    :param df: df with sentence embeddings
    :param ticker_column: the ticker column
    :param date_column: the date column
    :param embedding_column: the embedding column
    :param look_back: how long should we look back
    :param min_similarity: the minimum similarity level
    :return: a simplified pd.DataFrame
    """
    ticker_list = list(set(df[ticker_column]))
    worker_num = cpu_count() - 1 if cpu_count() > 1 else 1
    task_list = [
        df[df[ticker_column] == ticker].reset_index(drop=True) for ticker in ticker_list
    ]
    partial_func = partial(
        delete_duplicate_news,
        look_back=look_back,
        date_column=date_column,
        embedding_column=embedding_column,
        min_similarity=min_similarity,
    )

    result_list = run_multitasking(
        func=partial_func,
        argument_list=task_list,
        num_workers=worker_num,
        thread_or_process="process",
    )

    result = (
        pd.concat(result_list).drop(columns=[embedding_column]).reset_index(drop=True)
    )
    return result


def add_embedding(
    df,
    model_name_or_path: str = "bert-base-nli-mean-tokens",
    target_column: str = "title",
):
    """
    :param df: original DataFrame
    :param model_name_or_path: which model to be applied
    :param target_column: the target column, where the text is stored
    """
    embedder = SentenceTransformer(model_name_or_path)
    target_list = df[target_column]
    embeddings = embedder.encode(target_list, batch_size=32, show_progress_bar=True)
    df["embeddings"] = list(embeddings)
    return df


def delete_duplicate_news(
    sub_df,
    look_back: int,
    date_column: str,
    embedding_column: str,
    min_similarity: float = 0.8,
):
    """
    Delete duplicate news for one single ticker.
    :param sub_df: df for a single ticker
    :param look_back: how long should we look back
    :param date_column: the date column
    :param embedding_column: the embedding column
    :param min_similarity: the minimum similarity level
    :return: new sub_df with only unique and fresh news
    """
    drop_index_list = []
    for index, row in sub_df.iterrows():
        # check whether we should do the duplicate check
        end_date = row[date_column]
        date_range = [
            end_date - pd.Timedelta(timedelta(days=i)) for i in range(look_back)
        ]
        # we select those news, whose dates are earlier (look_back days) and not yet been kicked out.
        indicator_list = [
            row_[date_column] in date_range
            and index_ < index
            and index_ not in drop_index_list
            for index_, row_ in sub_df.iterrows()
        ]
        check_news = sub_df.loc[indicator_list, :]
        if len(check_news) != 0:
            query_embedding = row[embedding_column]
            corpus_embeddings = list(check_news[embedding_column])
            distances = scipy.spatial.distance.cdist(
                [query_embedding], corpus_embeddings, "cosine"
            )[0]
            scores = [1 - distance for distance in distances]
            if max(scores) >= min_similarity:
                drop_index_list.append(index)
    sub_df = sub_df.drop(index=drop_index_list)
    return sub_df
