"""
Show how to get data from open-source news databases
"""

# 1. get data from the general gdelt database

from NewsTrader.newsfeed.gdeltdatabase import GdeltDatabase


# three sub databases are available: events, gkg, and mentions
gdelt_data = GdeltDatabase(date="2021-09-01", table="events").query()
gdelt_data.head()


