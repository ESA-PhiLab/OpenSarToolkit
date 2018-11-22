#! /usr/bin/env python
"""
This script provides core functionalities for the phiSAR package.
"""

# import stdlib modules
import os
import shlex
import subprocess
import time
import datetime
from datetime import timedelta

# script infos
__author__ = 'Andreas Vollrath'
__copyright__ = 'phi-lab, European Space Agency'

__license__ = 'GPL'
__version__ = '1.0'
__maintainer__ = 'Andreas Vollrath'
__email__ = ''
__status__ = 'Production'


def getGPT():
    """
    This function looks for the most common places where SNAP's gpt executable
    is stored and returns its path.
    """

    if os.name == 'nt':
        if os.path.isfile('C:\\Program Files\\snap\\bin\\gpt'):
            gptfile = 'C:\\Program Files\\snap\\bin\\gpt'
        else:
            gptfile = input(' Please provide the full path to the'
                            ' SNAP gpt command line executable'
                            ' (e.g. C:\\path\\to\snap\\bin\gpt)')
    else:
        homedir = os.getenv("HOME")
        if os.path.isfile('{}/.ost/gpt'.format(homedir)):
            gptfile = '{}/.ost/gpt'.format(homedir)
        elif os.path.isfile('/usr/bin/gpt'):
            gptfile = '/usr/bin/gpt'
        elif os.path.isfile('/opt/snap/bin/gpt'):
            gptfile = '/opt/snap/bin/gpt'
        elif os.path.isfile('/usr/local/snap/bin/gpt'):
            gptfile = '/usr/local/snap/bin/gpt'
        elif os.path.isfile('/usr/local/lib/snap/bin/gpt'):
            gptfile = '/usr/local/lib/snap/bin/gpt'
        elif os.path.isfile('{}/snap/bin/gpt'.format(homedir)):
            gptfile = '{}/snap/bin/gpt'.format(homedir)
        elif os.path.isfile('/Applications/snap/bin/gpt'):
            gptfile = '/Applications/snap/bin/gpt'
        else:
            gptfile = input(' Please provide the full path to the SNAP'
                            ' gpt command line executable'
                            ' (e.g. /path/to/snap/bin/gpt')

    print(' INFO: using SNAP CL executable at {}'.format(gptfile))
    return gptfile

# def get TMP():
#     """
#     This functions looks for the best temp folder.
#     """


def is_valid_directory(parser, arg):
    if not os.path.isdir(arg):
        parser.error('The directory {} does not exist!'.format(arg))
    else:
        # File exists so return the directory
        return arg


def is_valid_file(parser, arg):
    if not os.path.isfile(arg):
        parser.error('The file {} does not exist!'.format(arg))
    else:
        # File exists so return the filename
        return arg


# check the validity of the date function
def is_valid_date(parser, arg):
    try:
        return datetime.datetime.strptime(arg, "%Y-%m-%d").date()
    except ValueError:
        parser.error("Not a valid date: '{0}'.".format(arg))


def is_valid_aoi(parser, arg):
    if arg is not '*':
        if not os.path.isfile(arg):
            parser.error('The file {} does not exist!'.format(arg))
        else:
            # File exists so return the filename
            return arg
    else:
        # return aoi as *
        return arg


def timer(start):
    elapsed = time.time() - start
    print(' INFO: Time elapsed: {}'.format(timedelta(seconds=elapsed)))


def runCmd(cmd, logFile):

    currtime = time.time()
    process = subprocess.run(shlex.split(cmd), stderr=subprocess.PIPE)

    if process.returncode != 0:
        with open(logFile, 'w') as f:
            for line in process.stderr.decode().splitlines():
                f.write('{}\n'.format(line))

    timer(currtime)
    return process.returncode
