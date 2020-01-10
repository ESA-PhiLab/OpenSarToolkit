import os
from os.path import join as opj
import math
import sys
import glob
import shlex
import shutil
import subprocess
import time
import gdal
import datetime
import logging
from datetime import timedelta
from pathlib import Path
import zipfile

from ost.s1.burst_to_ard import burst_to_ard

logger = logging.getLogger(__name__)


def gpt_path():
    '''An automatic finder for SNAP'S gpt command line executable

    This function looks for the most common places where SNAP's gpt executable
    is stored and returns its path.

    If no file could be found, it will ask for the location.

    Returns:
        path to SNAP's gpt command line executable
    '''
    gptfile = None
    if os.name == 'nt':
        if Path(r'c:/Program Files/snap/bin/gpt.exe').is_file() is True:
            gptfile = Path(r'c:/Program Files/snap/bin/gpt.exe')
        else:
            gptfile = input(r'Please provide the full path to the'
                            r'SNAP gpt command line executable'
                            r'(e.g. C:\path\to\snap\bin\gpt.exe)')
            gptfile = Path(gptfile)

            if gptfile.is_file() is False:
                logger.debug('ERROR: path to gpt file is incorrect. No such file.')
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
            '/Applications/snap/bin/gpt',
            homedir+'/SNAP_Toolbox/snap/bin/gpt',
            homedir+'/snap/bin/gpt'
        ]

        for path in paths:
            if os.path.isfile(path):
                gptfile = path
                break

    if gptfile is None:
        gptfile = os.getenv('GPT_PATH')
        os.path.isfile(gptfile)

    if not gptfile:
        gptfile = input('Please provide the full path to the SNAP'
                        'gpt command line executable'
                        '(e.g. /path/to/snap/bin/gpt')

        os.makedirs(opj(homedir, '.ost'), exist_ok=True)
        os.symlink(gptfile, opj(homedir, '.ost', 'gpt'))

    if os.path.isfile(gptfile) is False:
        logger.debug('ERROR: path to gpt file is incorrect. No such file.')
        sys.exit()

    # logger.debug('INFO: using SNAP CL executable at {}'.format(gptfile))
    return gptfile


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
    '''A helper function to logger.debug a time elapsed statement

    Args:
        start (time): a time class object for the start time

    '''

    elapsed = time.time() - start
    logger.debug('INFO: Time elapsed: {}'.format(timedelta(seconds=elapsed)))


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


def run_command(command, logfile, elapsed=True, silent=False):
    '''A helper function to execute a command line command

    Args:
        command (str): the command to execute
        logfile (str): path to the logfile in case of errors
        silent (bool): if you want to shut GPT up or not

    '''

    currtime = time.time()
    if silent:
        dev_null = open(os.devnull, 'w')
        stderr = subprocess.STDOUT
    else:
        stderr = subprocess.PIPE
        dev_null = None

    if os.name == 'nt':
        process = subprocess.run(command,
                                 stdout=dev_null,
                                 stderr=stderr
                                 )
    else:
        process = subprocess.run(shlex.split(command),
                                 stderr=stderr,
                                 stdout=dev_null,
                                 )

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

    shutil.rmtree('{}.data'.format(dimap_prefix))
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

    if dim_size_in_mb < 0.2:
        return 666

    for file in glob.glob(opj('{}.data'.format(dimap_prefix), '*.img')):

        # check size
        data_size_in_mb = os.path.getsize(file) / 1048576

        if data_size_in_mb < 0.2:
            logger.debug('data small')
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
    if tiff_size_in_mb < 0.2:
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
        logger.debug('Error: {}'.format(er))
        return 1
    
    try:
        zip_test = zip_archive.testzip()
    except:
        logger.debug('Error')
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


def zip_s1_safe_dir(dir_path, zip_path, product_id):
    zipf = zipfile.ZipFile(zip_path, mode='w')
    len_dir = len(dir_path)
    for root, _, files in os.walk(dir_path):
        logger.debug(root, _, files)
        for file in files:
            file_path = opj(root, file)
            if '.downloaded' not in zip_path:
                zipf.write(file_path, product_id+'.SAFE'+file_path[len_dir:])
    zipf.close()


def _slc_zip_to_processing_dir(
        processing_dir,
        product,
        product_path
):
    import shutil
    download_path = os.path.join(processing_dir, 'SAR',
                                 product.product_type,
                                 product.year,
                                 product.month,
                                 product.day
                                 )
    os.makedirs(download_path, exist_ok=True)

    if not os.path.exists(
            os.path.join(download_path, os.path.basename(product_path))
    ):
        shutil.copy(product_path, download_path)
        with open(
                os.path.join(
                    download_path, os.path.basename(product_path)
                )+'.downloaded', 'w'
        ) as zip_dl:
            zip_dl.write('1')


def execute_ard(
        burst,
        swath,
        master_file,
        out_dir,
        temp_dir,
        scene_id,
        ard_parameters
):
    m_nr, m_burst_id, b_bbox = burst
    return_code = burst_to_ard(
        master_file=master_file,
        swath=swath,
        master_burst_nr=m_nr,
        master_burst_id=str(m_burst_id),
        master_burst_poly=b_bbox,
        out_dir=out_dir,
        out_prefix=scene_id,
        temp_dir=temp_dir,
        slave_file=None,
        slave_burst_nr=None,
        slave_burst_id=None,
        polarimetry=False,
        pol_speckle_filter=False,
        resolution=ard_parameters['resolution'],
        product_type=ard_parameters['product_type'],
        speckle_filter=ard_parameters['speckle_filter'],
        # To_db not working
        to_db=ard_parameters['to_db'],
        ls_mask_create=False,
        dem=ard_parameters['dem'],
        remove_slave_import=False
    )
    if return_code != 0:
        raise RuntimeError(
            'Something went wrong with the GPT processing! with return code: %s' %
            return_code
        )
    out_file = opj(out_dir, '_'.join([
        scene_id, str(m_burst_id), 'BS.dim'
    ])
                   )
    if not os.path.isfile(out_file):
        raise RuntimeError
    return return_code, out_file
