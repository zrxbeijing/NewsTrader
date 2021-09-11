import requests
import os
import pandas as pd
import json
import random
from functools import partial
from multiprocessing import cpu_count
from gzip import decompress
from NewsTrader.utils.accelerator import run_multitasking


class GegDatabase:
    """
    GDELT geg database is kind of special as it offers sentiment score.
    This class provides a shortcut for getting the data from geg database.
    """

    _MASTER_FILE_URL = (
        "http://data.gdeltproject.org/gdeltv3/geg_gcnlapi/MASTERFILELIST.TXT"
    )

    def __init__(self, start, end, num_process, master_file_dir="./", lang="en"):
        self.start = pd.to_datetime(start)
        self.end = pd.to_datetime(end)
        self.num_process = num_process
        self.master_file_dir = master_file_dir
        self.master_file = os.path.join(self.master_file_dir, "MASTERFILELIST.TXT")
        self.lang = lang

    def _get_master_file(self):
        """
        Download the master file from geg database
        """
        response = requests.get(url=GegDatabase._MASTER_FILE_URL)
        if not response.status_code == 0:
            print(
                "There is something wrong with the original url for the master file! Check GDELT GEG!"
            )
        if not os.path.exists(self.master_file_dir):
            os.mkdir(self.master_file_dir)

        with open(self.master_file, "w") as file:
            file.write(response.text)
        print("Successfully downloaded master file!")

    def get_table(self):
        """
        Parse the master file into a DataFrame
        :return:
        """
        if not os.path.exists(self.master_file):
            self._get_master_file()

        with open(self.master_file, "r") as file:
            lines = file.readlines()
        json_file_list = [file.strip() for file in lines]

        json_date_list = [
            file.split("/")[-1].split(".")[0][:8] for file in json_file_list
        ]
        start = self.start.strftime(format="%Y%m%d")
        end = (self.end + pd.Timedelta(value=1, unit="D")).strftime(format="%Y%m%d")
        json_file_list = json_file_list[
            json_date_list.index(start) : json_date_list.index(end)
        ]

        if self.num_process is None:
            worker_num = 1
        else:
            if isinstance(self.num_process, int):
                worker_num = self.num_process
            else:
                worker_num = cpu_count() - 1 if cpu_count() > 1 else 1

        partial_func = partial(self._parse_one_entry)
        # apply multiprocessing to speed up (cpu intensive task)
        result_list = run_multitasking(
            func=partial_func,
            argument_list=json_file_list,
            num_workers=worker_num,
            thread_or_process="process",
        )
        result = [record for result in result_list for record in result]
        return pd.DataFrame(result)

    @staticmethod
    def _parse_one_entry(json_file, lang="en"):
        """
        Parse one single geg entry, ready to be used by the multiprocessing process in _parse_master_file
        :param json_file: one geg entry in the master file
        :param lang: str, language
        :return:
        """
        response = requests.get(json_file)
        json_string = decompress(response.content).decode("utf-8").strip()
        entry_list = json_string.split("\n")
        entry_list = [
            entry.split(', "entities"')[0].strip() + "}" for entry in entry_list
        ]
        entry_list = [json.loads(entry) for entry in entry_list]
        # randomize urls
        entry_list = random.choices(entry_list, k=len(entry_list))
        # select language
        entry_list = [entry for entry in entry_list if entry["lang"] == lang]
        return entry_list
