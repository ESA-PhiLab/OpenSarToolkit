#! /usr/bin/env python
"""
This script provides core functionalities for the OST package.
"""

# import stdlib modules
import os
from os.path import join as opj
import math
import sys
import glob
import shlex
import shutil
import subprocess
import time
import datetime
from datetime import timedelta
from pathlib import Path
import zipfile

import gdal

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
            '{}/snap/bin/gpt'.format(homedir),
            '{}/programs/snap/bin/gpt'.format(homedir),
            '/usr/bin/gpt',
            '/opt/snap/bin/gpt',
            '/usr/local/snap/bin/gpt',
            '/usr/local/lib/snap/bin/gpt',
            '/Applications/snap/bin/gpt'
            ]

        for path in paths:
            if os.path.isfile(path):
                gptfile = path
                return gptfile
            else:
                gptfile = None
                
    # chek if we have an enviromenral variable that contains the path to gpt
    if not gptfile:
        gptfile = os.getenv('GPT_PATH')
        
    if not gptfile:
        gptfile = input(' Please provide the full path to the SNAP'
                        ' gpt command line executable'
                        ' (e.g. /path/to/snap/bin/gpt')

        if os.path.isfile(gptfile) is False:
            print(' ERROR: path to gpt file is incorrect. No such file.')
            sys.exit()
        else:
            os.makedirs(opj(homedir, '.ost'), exist_ok=True)
            os.symlink(gptfile, opj(homedir, '.ost', 'gpt'))
            gptfile = opj(homedir, '.ost', 'gpt')
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


def remove_folder_content(folder):
    '''A helper function that cleans the content of a folder

    Args:
        folder: the folder, where everything should be deleted
    '''

    for root, dirs, files in os.walk(folder):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


def run_command(command, logfile, elapsed=True):
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

    if elapsed:
        timer(currtime)
        
    return process.returncode


def delete_dimap(dimap_prefix):
    '''Removes both dim and data from a Snap dimap file

    '''

    if os.path.isdir('{}.data'.format(dimap_prefix)):
        shutil.rmtree('{}.data'.format(dimap_prefix))
    
    if os.path.isfile('{}.dim'.format(dimap_prefix)):
        os.remove('{}.dim'.format(dimap_prefix))


def delete_shapefile(shapefile):
    '''Removes the shapefile and all its associated files

    '''

    extensions = ('.shp', '.prj', '.shx', '.dbf', '.cpg', 'shb')

    for file in glob.glob('{}*'.format(os.path.abspath(shapefile[:-4]))):

        if len(os.path.abspath(shapefile)) == len(file):

            if file.endswith(extensions):
                os.remove(file)


def move_dimap(infile_prefix, outfile_prefix):
    '''This function moves a dima file to another locations


    '''

    if os.path.isdir('{}.data'.format(outfile_prefix)):
        delete_dimap(outfile_prefix)

    out_dir = os.path.split('{}.data'.format(outfile_prefix))[:-1][0]

    # move them to the outfolder
    shutil.move('{}.data'.format(infile_prefix), out_dir)
    shutil.move('{}.dim'.format(infile_prefix),
                '{}.dim'.format(outfile_prefix))


def check_out_dimap(dimap_prefix, test_stats=True):

    return_code = 0

    # check if both dim and data exist, else return
    if not os.path.isfile('{}.dim'.format(dimap_prefix)):
        return 666

    if not os.path.isdir('{}.data'.format(dimap_prefix)):
        return 666

    # check for file size of the dim file
    dim_size_in_mb = os.path.getsize('{}.dim'.format(dimap_prefix)) / 1048576

    if dim_size_in_mb < 1:
        return 666

    for file in glob.glob(opj('{}.data'.format(dimap_prefix), '*.img')):

        # check size
        data_size_in_mb = os.path.getsize(file) / 1048576

        if data_size_in_mb < 1:
            return 666

        if test_stats:
            # open the file
            ds = gdal.Open(file)
            stats = ds.GetRasterBand(1).GetStatistics(0, 1)

            # check for mean value of layer
            if stats[2] == 0:
                return 666

            # check for stddev value of layer
            if stats[3] == 0:
                return 666

            # if difference ofmin and max is 0
            if stats[1] - stats[0] == 0:
                return 666

    return return_code


def check_out_tiff(file, test_stats=True):

    return_code = 0

    # check if both dim and data exist, else return
    if not os.path.isfile(file):
        return 666

    # check for file size of the dim file
    tiff_size_in_mb = os.path.getsize(file) / 1048576

    if tiff_size_in_mb < 0.3:
        return 666

    if test_stats:
        # open the file
        ds = gdal.Open(file)
        stats = ds.GetRasterBand(1).GetStatistics(0, 1)

        # check for mean value of layer
        if stats[2] == 0:
            return 666

        # check for stddev value of layer
        if stats[3] == 0:
            return 666

        # if difference ofmin and max is 0
        if stats[1] - stats[0] == 0:
            return 666

    return return_code


def check_zipfile(filename):
        
    try:
        zip_archive = zipfile.ZipFile(filename)
    except zipfile.BadZipFile as er:
        print('Error: {}'.format(er))
        return 1
    
    try:
        zip_test = zip_archive.testzip()
    except:
        print('Error')
        return 1
    else:
        return zip_test
    
def resolution_in_degree(latitude, meters):
    '''Convert resolution in meters to degree based on Latitude

    '''

    earth_radius = 6378137
    degrees_to_radians = math.pi/180.0
    radians_to_degrees = 180.0/math.pi
    "Given a latitude and a distance west, return the change in longitude."
    # Find the radius of a circle around the earth at given latitude.
    r = earth_radius*math.cos(latitude*degrees_to_radians)
    return (meters/r)*radians_to_degrees


def test_ard_parameters(ard_parameter_dict):
    
    # snap things
    resampling = ['NEAREST_NEIGHBOUR', 'BILINEAR_INTERPOLATION', 
                  'CUBIC_CONVOLUTION', 'BISINC_5_POINT_INTERPOLATION', 
                  'BISINC_11_POINT_INTERPOLATION', 
                  'BISINC_21_POINT_INTERPOLATION', 'BICUBIC_INTERPOLATION']
    window_sizes = ['3x3', '5x5']
    target_window_sizes = ['5x5', '7x7', '9x9', '11x11', '13x13', 
                           '15x15', '17x17']
    speckle_filters = ['None', 'Boxcar', 'Median', 'Frost', 'Gamma Map', 
                       'Lee', 'Refined Lee', 'Lee Sigma', 'IDAN']
    damping = range(0,100)
    pan_size = range(0,200)
    filter_size_x = range(0,100)
    filter_size_y = range(0,100)
    nr_of_looks = range(1,4)
    sigma = [0.5, 0.6, 0.7, 0.8, 0.9]
    
    # ost things
    grd_types = ['CEOS', 'Earth Engine', 'OST Standard']
    slc_types = ['OST Standard','OST Plus', 'OST Minimal']
    product_types = ['RTC', 'GTCsigma', 'GTCgamma']
    metrics = ['median', 'percentiles', 'harmonics', 
               'avg', 'max', 'min', 'std', 'cov']
    datatypes = ['float32', 'uint8', 'uint16']
    
    #    assert ard_parameter_dict['resolution']