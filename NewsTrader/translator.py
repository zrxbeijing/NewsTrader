"""
This module provides the functionality of translation.
Translation often takes very long time.
We utilize the argotranslate package to speed up local translation.
""" 
from argostranslate import package, translate
from argostranslate import settings


def truncate_text(text, size):
    """
    truncate the text to a given length.
    """
    word_list = text.split()
    if len(word_list) > int(size):
        word_list = word_list[0 : int(size)]
    return " ".join(word_list)


def translate(data_df, model_path, device):
    """
    Local translation using argotranslate package.
    """
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
