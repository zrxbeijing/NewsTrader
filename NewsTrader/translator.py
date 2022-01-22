# decide news language
from langdetect import detect
import pandas as pd
from NewsTrader.utils.accelerator import run_multitasking
from functools import partial
from argostranslate import package, translate
from argostranslate import settings
import time
import pandas as pd


# truncate the text to shorter length
def truncate_text(text, size):
    word_list = text.split()
    if len(word_list) > int(size):
        word_list = word_list[0 : int(size)]
    return " ".join(word_list)


def translate(data_df, model_path, device):
    if device == "gpu":
        settings.device = "cuda"

    package.install_from_path(model_path)
    installed_languages = translate.get_installed_languages()
    translation_a_b = installed_languages[1].get_translation(installed_languages[0])

    translation_result = []
    for index, row in data_df.iterrows():
        print(index)
        translated = translation_a_b.translate(row["truncated_text"])
        translation_result.append(translated)

    return translation_result
