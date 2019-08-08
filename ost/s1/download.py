# -*- coding: utf-8 -*-
'''
This module handles the download of Sentinel-1, offering download capabilities
from different servers such as Copernicus Scihub, Alaska Satellite Facility's
vertex as well as PEPS from CNES.
'''

# import stdlib modules
import os
import glob
import getpass
import multiprocessing

from os.path import join as opj

# import external modules
import pandas as pd

# import OST libs
from ost.s1.s1scene import S1Scene
from ost.helpers import scihub, peps, asf

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
    for scene in glob.glob('{}/*zip'.format(input_directory)):

        # get scene
        scene = S1Scene(os.path.basename(scene)[:-4])

        # create download path and file
        download_path = opj(download_dir, 'SAR', scene.product_type,
                            scene.year, scene.month, scene.day)

        # create download dir
        os.makedirs(download_path, exist_ok=True)
        outfile = opj(download_path, '{}.zip'.format(scene.scene_id))

        # move file
        os.rename(scene, outfile)


def check_scene_availability(inventory_df, download_dir, cloud_provider=None):
    '''Function to check if a scene is on a cloud storage

    This function checks for the availability of scenes present in an
    OST compliant inventory GeodataFrame object on different cloud providers
    and adds flags the ones that need to be downloaded.


    Args:
        inventory_df: a Geopandas GeoDataFrame object originating from
                      an OST search and/or possible search refinement-sorting
        download_dir: is the directory where scenes should be downloaded
        cloud_provider: defines on which cloud we operate.
                       Possible choices:
                               - Creo
                               - AWS
                               - Mundi
                               - ONDA

    Returns:
        GeoDataFrame: An updated OST compliant inventory GeoDataFrame object
                      with an additional column that contains a flag wether
                      a scene needs to be downloaded or can be directly
                      accessed via the cloud provider.


    Notes:
        Should be applied after read_inventory and before download

    '''

    print(' INFO: Checking for availability of scenes on {}.'.format(
        cloud_provider))
    # create an empty DataFrame
    download_df = pd.DataFrame(
            columns=['identifier', 'filepath', 'toDownload'])

    # loop through each scene
    scenes = inventory_df['identifier'].tolist()
    for scene_id in scenes:

        scene = S1Scene(scene_id)

        # check if we can download from the cloud_provider
        if cloud_provider == 'Creo':
            test_path = scene.creodias_path()
        elif cloud_provider == 'AWS':
            test_path = scene.aws_path()   # function needs to be added
        elif cloud_provider == 'Mundi':
            test_path = scene.mundi_path()   # function needs to be added
        else:
            # construct download path
            test_path = opj(download_dir, 'SAR', scene.product_type,
                            scene.year, scene.month, scene.day, scene_id)

        # check for existence of files
        if os.path.isdir(test_path) or os.path.exists(test_path):

            # if we are not in cloud
            if download_dir in test_path:

                # file is already succesfully downloaded
                download_df = download_df.append({'identifier': scene_id,
                                                  'filepath': test_path,
                                                  'toDownload': False},
                                                 ignore_index=True)
            else:

                # file is on cloud storage
                download_df = download_df.append({'identifier': scene_id,
                                                  'filepath': test_path,
                                                  'toDownload': False},
                                                 ignore_index=True)

        else:

            # construct download path to check if we already downloaded
            test_path = opj(download_dir, 'SAR', scene.product_type,
                            scene.year, scene.month, scene.day, scene_id)

            # if we are on cloud, check if we already downloaded
            if os.path.exists(test_path):

                # file is already succesfully downloaded
                download_df = download_df.append({'identifier': scene_id,
                                                  'filepath': test_path,
                                                  'toDownload': False},
                                                 ignore_index=True)

            else:
                download_df = download_df.append({'identifier': scene_id,
                                                  'filepath': test_path,
                                                  'toDownload': True},
                                                 ignore_index=True)

    # merge the dataframe and return it
    inventory_df = inventory_df.merge(download_df, on='identifier')
    return inventory_df


def download_sentinel1(inventory_df, download_dir, mirror=None, concurrent=2,
                       uname=None, pword=None):
    '''Main function to download Sentinel-1 data

    This is an interactive function

    '''

    if not mirror:
        print(' INFO: One or more of your scenes need to be downloaded.')
        print(' Select the server from where you want to download:')
        print(' (1) Copernicus Apihub (ESA, rolling archive)')
        print(' (2) Alaska Satellite Facility (NASA, full archive)')
        print(' (3) PEPS (CNES, 1 year rolling archive)')
        mirror = input(' Type 1, 2 or 3: ')

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

    if int(mirror) == 1:
        # check response
        if error_code == 401:
            raise ValueError(' ERROR: Username/Password are incorrect.')
        elif error_code != 200:
            raise ValueError(' Some connection error. Error code: {}'.format(
                    error_code))

        # check if all scenes exist
        scenes = inventory_df['identifier'].tolist()

        download_list = []
        asf_list = []

        for scene_id in scenes:
            scene = S1Scene(scene_id)
            download_path = opj(download_dir, 'SAR', scene.product_type,
                                scene.year, scene.month, scene.day)

            filename = '{}.zip'.format(scene.scene_id)

            uuid = (inventory_df['uuid']
                    [inventory_df['identifier'] == scene_id].tolist())

            if os.path.isdir(download_path) is False:
                os.makedirs(download_path)

            # in case the data has been downloaded before
            # if os.path.exists('{}/{}'.format(download_path, filename))
            # is False:
            # create list objects for download
            download_list.append([uuid[0], '{}/{}'.format(
                download_path, filename), uname, pword])
            asf_list.append([scene.asf_url(), '{}/{}'.format(
                download_path, filename), uname, pword])

        # download in parallel
        if int(mirror) == 1:   # scihub
            pool = multiprocessing.Pool(processes=2)
            pool.map(scihub.s1_download, download_list)
    elif int(mirror) == 2:    # ASF
        asf.batch_download(inventory_df, download_dir,
                           uname, pword, concurrent)
    elif int(mirror) == 3:   # PEPS
        peps.batch_download(inventory_df, download_dir,
                            uname, pword, concurrent)
