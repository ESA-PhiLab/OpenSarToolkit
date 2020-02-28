import importlib
from ost.helpers import helpers as h
import logging


# ---------------------------------------------------------------
# logger set-up
# lower stream output log level
class SingleLevelFilter(logging.Filter):
    def __init__(self, passlevel, reject):
        self.passlevel = passlevel
        self.reject = reject

    def filter(self, record):
        if self.reject:
            return (record.levelno != self.passlevel)
        else:
            return (record.levelno == self.passlevel)


formatter = logging.Formatter(' %(levelname)s (%(asctime)s): %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)


def set_log_level(loglevel=logging.INFO):

    if loglevel == logging.INFO:
        info_filter = SingleLevelFilter(logging.INFO, False)
        stream_handler.addFilter(info_filter)
        logging.getLogger().addHandler(stream_handler)


def setup_logfile(logfile):
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
# ---------------------------------------------------------------

# ---------------------------------------------------------------
# GLOBAL VARIABLES


# get gpt file
GPT_FILE = h.gpt_path()

# get path to graph
OST_ROOT = importlib.util.find_spec('ost').submodule_search_locations[0]
# ---------------------------------------------------------------

# ---------------------------------------------------------------
# dummy user for tests
HERBERT_USER = {'uname': 'herbert_thethird',
                'pword': 'q12w34er56ty7',
                'asf_pword': 'q12w34er56ty7WER32P'
                }
# ---------------------------------------------------------------