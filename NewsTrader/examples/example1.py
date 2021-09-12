"""
Show how to get data from open-source news databases
"""

# 1. get data from the general gdelt database

from NewsTrader.newsfeed.gdeltdatabase import GdeltDatabase


# three sub databases are available: events, gkg, and mentions
gdelt_data = GdeltDatabase(date="2021-09-01", table="events").query()
gdelt_data.head()


# 2. get data from the geg database

from NewsTrader.newsfeed.gegdatabase import GegDatabase


geg_data = GegDatabase(start="2021-09-01", end="2021-09-01", num_process=8).get_table()
