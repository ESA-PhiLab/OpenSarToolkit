# -*- coding: utf-8 -*-
'''
This module handles the download of Sentinel-1, offering download capabilities
from different servers such as Copernicus Scihub, Alaska Satellite Facility's
vertex as well as PEPS from CNES.
'''

# import stdlib modules
import os
from os.path import join as opj
import glob
import getpass

# import OST libs
from ost.s1.s1scene import Sentinel1_Scene as S1Scene
from ost.helpers import scihub, peps, asf, onda

# script infos
__author__ = 'Andreas Vollrath'
__copyright__ = 'phi-lab, European Space Agency'

__license__ = 'GPL'
__version__ = '1.0'
__maintainer__ = 'Andreas Vollrath'
__email__ = ''
__status__ = 'Production'


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
        print(' INFO: Checking zip file {} for inconsistency.'.format(scene_in))
        zip_test = h.check_zipfile(scene_in)
        
        if not zip_test:
            print(' INFO: Passed')
            # move file
            os.rename(scene_in, filepath)
        
            # add downloaded (should be zip checked in future)
            f=open(filepath+".downloaded","w+")
            f.close()
        else:
            print(' INFO: File {} is corrupted and will not be moved.')


def download_sentinel1(inventory_df, download_dir, mirror=None, concurrent=2,
                       uname=None, pword=None):
    '''Main function to download Sentinel-1 data

    This is an interactive function

    '''

    if not mirror:
        print(' Select the server from where you want to download:')
        print(' (1) Copernicus Apihub (ESA, rolling archive)')
        print(' (2) Alaska Satellite Facility (NASA, full archive)')
        print(' (3) PEPS (CNES, 1 year rolling archive)')
        print(' (4) ONDA DIAS (ONDA DIAS full archive for SLC - or GRD from 30 June 2019)')
        mirror = input(' Type 1, 2, 3 or 4: ')

    if not uname:
        print(' Please provide username for the selected server')
        uname = input(' Username:')

    if not pword:
        print(' Please provide password for the selected server')
        pword = getpass.getpass(' Password:')

    # check if uname and pwrod are correct
    if int(mirror) == 1:
        error_code = scihub.check_connection(uname, pword)
    elif int(mirror) == 2:
        error_code = asf.check_connection(uname, pword)

        if concurrent > 10:
            print(' INFO: Maximum allowed parallel downloads \
                  from Earthdata are 10. Setting concurrent accordingly.')
            concurrent = 10
    
    elif int(mirror) == 3:
        error_code = peps.check_connection(uname, pword)
    elif int(mirror) == 4:
        error_code = onda.check_connection(uname, pword)
        
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
    elif int(mirror) == 4:   # ONDA DIAS
        onda.batch_download(inventory_df, download_dir,
                            uname, pword, concurrent)
