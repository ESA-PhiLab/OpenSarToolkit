'''
This module handles the download of Sentinel-1, offering download capabilities
from different servers such as Copernicus Scihub, Alaska Satellite Facility's
vertex as well as PEPS from CNES.
'''
import os
from os.path import join as opj
import glob
import logging
import getpass

from ost.s1.s1scene import Sentinel1Scene as S1Scene
from ost.helpers import scihub, peps, asf

logger = logging.getLogger(__name__)


def restore_download_dir(input_directory, download_dir):
    '''Function to create the OST download directory structure

    In case data is already downloaded to a single folder, this function can
    be used to create a OST compliant structure of the download directory.

    Args:
        input_directory: the directory, where the dwonloaded files are located
        download_dir: the high-level directory compliant with OST

    '''
    
    from ost.helpers import helpers as h
    
    for scene_in in glob.glob(opj(input_directory, '*zip')):

        # get scene
        scene = S1Scene(os.path.basename(scene_in)[:-4])

        # create download path and file
        filepath = scene._download_path(download_dir, True)

        # check zipfile
        logger.debug('INFO: Checking zip file {} for inconsistency.'.format(scene_in))
        zip_test = h.check_zipfile(scene_in)
        
        if not zip_test:
            logger.debug('INFO: Passed')
            # move file
            os.rename(scene_in, filepath)
        
            # add downloaded (should be zip checked in future)
            f = open(filepath+".downloaded","w+")
            f.close()
        else:
            logger.debug('INFO: File {} is corrupted and will not be moved.')


def download_sentinel1(inventory_df, download_dir, mirror=None, concurrent=2,
                       uname=None, pword=None):
    '''Main function to download Sentinel-1 data

    This is an interactive function

    '''

    if not mirror:
        logger.debug('Select the server from where you want to download:')
        logger.debug('(1) Copernicus Apihub (ESA, rolling archive)')
        logger.debug('(2) Alaska Satellite Facility (NASA, full archive)')
        logger.debug('(3) PEPS (CNES, 1 year rolling archive)')
        mirror = input(' Type 1, 2 or 3: ')

    if not uname:
        logger.debug('Please provide username for the selected server')
        uname = input(' Username:')

    if not pword:
        logger.debug('Please provide password for the selected server')
        pword = getpass.getpass('Password:')

    # check if uname and pwrod are correct
    if int(mirror) == 1:
        error_code = scihub.check_connection(uname, pword)
    elif int(mirror) == 2:
        error_code = asf.check_connection(uname, pword)

        if concurrent > 10:
            logger.debug('INFO: Maximum allowed parallel downloads \
                  from Earthdata are 10. Setting concurrent accordingly.')
            concurrent = 10
    
    elif int(mirror) == 3:
        error_code = peps.check_connection(uname, pword)

    if error_code == 401:
        raise ValueError(' ERROR: Username/Password are incorrect')
    elif error_code != 200:
        raise ValueError(' ERROR: Some connection error. Error code {}.'.format(error_code))
    
    # download in parallel
    if int(mirror) == 1:
        scihub.batch_download(inventory_df, download_dir,
                              uname, pword, concurrent) # scihub
    elif int(mirror) == 2:    # ASF
        asf.batch_download(inventory_df, download_dir,
                           uname, pword, concurrent)
    elif int(mirror) == 3:   # PEPS
        peps.batch_download(inventory_df, download_dir,
                            uname, pword, concurrent)
