import importlib
from ost.helpers import helpers as h

# get gpt file
GPT_FILE = h.gpt_path()

# get path to graph
OST_ROOT = importlib.util.find_spec('ost').submodule_search_locations[0]

HERBERT_USER = {'uname': 'herbert_thethird',
                'pword': 'q12w34er56ty7',
                'asf_pword': 'q12w34er56ty7WER32P'
                }
