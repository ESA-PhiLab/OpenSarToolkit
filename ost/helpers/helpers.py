#! /usr/bin/env python
"""
This script provides core functionalities for the OST package.
"""

# import stdlib modules
import os
from os.path import join as opj
import sys
import shlex
import shutil
import subprocess
import time
import datetime
from datetime import timedelta
from pathlib import Path


# script infos
__author__ = 'Andreas Vollrath'
__copyright__ = 'phi-lab, European Space Agency'

__license__ = 'GPL'
__version__ = '1.0'
__maintainer__ = 'Andreas Vollrath'
__email__ = ''
__status__ = 'Production'


def gpt_path():
    '''An automatic finder for SNAP'S gpt command line executable

    This function looks for the most common places where SNAP's gpt executable
    is stored and returns its path.

    If no file could be found, it will ask for the location.

    Returns:
        path to SNAP's gpt command line executable
    '''

    if os.name == 'nt':
        if Path(r'c:/Program Files/snap/bin/gpt.exe').is_file() is True:
            gptfile = Path(r'c:/Program Files/snap/bin/gpt.exe')
        else:
            gptfile = input(r' Please provide the full path to the'
                            r' SNAP gpt command line executable'
                            r' (e.g. C:\path\to\snap\bin\gpt.exe)')
            gptfile = Path(gptfile)

            if gptfile.is_file() is False:
                print(' ERROR: path to gpt file is incorrect. No such file.')
                sys.exit()
    else:
        homedir = os.getenv("HOME")
        # possible UNIX paths
        paths = [
            '{}/.ost/gpt'.format(homedir),
            '/usr/bin/gpt',
            '/opt/snap/bin/gpt',
            '/usr/local/snap/bin/gpt',
            '/usr/local/lib/snap/bin/gpt',
            '{}/snap/bin/gpt'.format(homedir),
            '/Applications/snap/bin/gpt'
            ]

        for path in paths:
            if os.path.isfile(path):
                gptfile = path
                break
            else:
                gptfile = None

    if not gptfile:
        gptfile = input(' Please provide the full path to the SNAP'
                        ' gpt command line executable'
                        ' (e.g. /path/to/snap/bin/gpt')
        shutil.copy(gptfile, opj(homedir, '.ost', 'gpt'))   # take care if .ost exists

    if os.path.isfile(gptfile) is False:
        print(' ERROR: path to gpt file is incorrect. No such file.')
        sys.exit()

    # print(' INFO: using SNAP CL executable at {}'.format(gptfile))
    return gptfile


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
    ''' A helper function to print a time elapsed statement

    Args:
        start (time): a time class object for the start time

    '''

    elapsed = time.time() - start
    print(' INFO: Time elapsed: {}'.format(timedelta(seconds=elapsed)))


def run_command(command, logfile):
    ''' A helper function to execute a command line command

    Args:
        command (str): the command to execute
        logfile (str): path to the logfile in case of errors

    '''

    currtime = time.time()

    if os.name == 'nt':
        process = subprocess.run(command, stderr=subprocess.PIPE)
    else:
        process = subprocess.run(shlex.split(command), stderr=subprocess.PIPE)

    return_code = process.returncode

    if return_code != 0:
        with open(str(logfile), 'w') as file:
            for line in process.stderr.decode().splitlines():
                file.write('{}\n'.format(line))

    timer(currtime)
    return process.returncode


def delete_dimap(file_prefix):
    '''
    Removes both dim and data from a Snap dimap file
    '''

    shutil.rmtree('{}.data'.format(file_prefix))
    os.remove('{}.dim'.format(file_prefix))


def move_dimap(infile_prefix, outfile_prefix):

    if os.path.isdir('{}.data'.format(outfile_prefix)):
        delete_dimap(outfile_prefix)

    out_dir = os.path.split('{}.data'.format(outfile_prefix))[:-1][0]
    print(out_dir)
    # move them to the outfolder
    shutil.move('{}.data'.format(infile_prefix), out_dir)
    shutil.move('{}.dim'.format(infile_prefix),
                '{}.dim'.format(outfile_prefix))
