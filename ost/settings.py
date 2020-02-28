import importlib
from ost.helpers import helpers as h

import logging

# lower stream output log level
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.ERROR)
logging.getLogger().addHandler(stream_handler)


def set_log_level(loglevel):
    logging.getLogger("ost").setLevel(loglevel)
    stream_handler.setLevel(loglevel)


def setup_logfile(logfile):
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)


# get gpt file
GPT_FILE = h.gpt_path()

# get path to graph
OST_ROOT = importlib.util.find_spec('ost').submodule_search_locations[0]

HERBERT_USER = {'uname': 'herbert_thethird',
                'pword': 'q12w34er56ty7',
                'asf_pword': 'q12w34er56ty7WER32P'
                }
