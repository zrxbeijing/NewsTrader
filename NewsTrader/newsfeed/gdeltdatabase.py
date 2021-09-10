import gdelt
import math
import time
import logging


logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)


class GdeltDatabase:
    """
    A database class wrapping the gdelt library.
    """

    def __init__(self,
                 date,
                 table,
                 max_retries=5):
        
        self.gd = gdelt.gdelt(version=2)
        self.date = date
        self.table = table
        self.max_retries = max_retries

    def query(self):
        """
        Basic query method.
        This method will try for several times util data is derived.
        """
        success = False
        result = None
        try_num = 0
        while not success:
            try:
                result = self.gd.Search(date=self.date, table=self.table, coverage=True)
                success = True
            except Exception as ex:
                if try_num < self.max_retries:
                    attempt = try_num + 1
                    sleep_time = int(math.pow(5, attempt))
                    logger.info("Retry attempt: ".format(str(attempt)))
                    logger.info("Sleep for {}".format(str(sleep_time)))
                    time.sleep(sleep_time)
                    try_num += 1
        return result
