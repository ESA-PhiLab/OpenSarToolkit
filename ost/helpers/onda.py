# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
'''
This module provides functions for downloading data from
ONDA Dias server.
'''

import os
from os.path import join as opj
import glob
import getpass
import multiprocessing
import urllib
import requests
import tqdm

from ost.helpers import helpers as h


def ask_credentials():
    '''A helper function that asks for user credentials on ONDA-DIAS

    Returns:
        uname: username of ONDA DIAS  page
        pword: password of ONDA DIAS  page

    '''
    # SciHub account details (will be asked by execution)
    print(' If you do not have a ONDA DIAS user account'
          ' go to: https://www.onda-dias.eu/cms/ and register')
    uname = input(' Your ONDA DIAS Username:')
    pword = getpass.getpass(' Your ONDA DIAS Password:')

    return uname, pword


def connect(base_url='https://catalogue.onda-dias.eu/dias-catalogue/',
            uname=None, pword=None):
    '''A helper function to connect and authenticate to the ONDA DIAS catalogue.

    Args:
        base_url (str): basic url to the ONDA DIAS catalogue
        uname (str): username of ONDA DIAS catalogue
        pword (str): password of ONDA DIAS catalogue

    Returns:
        opener: an urllib opener instance to ONDA DIAS catalogue

    '''

    if not uname:
        print(' If you do not have a ONDA DIAS user'
              ' account go to: https://www.onda-dias.eu/cms/')
        uname = input(' Your ONDA DIAS Username:')

    if not pword:
        pword = getpass.getpass(' Your ONDA DIAS Password:')

    # open a connection to the scihub
    manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    manager.add_password(None, base_url, uname, pword)
    handler = urllib.request.HTTPBasicAuthHandler(manager)
    opener = urllib.request.build_opener(handler)

    return opener


def check_connection(uname, pword):
    '''A helper function to check if a connection  can be established

    Args:
        uname: username of ONDA DIAS page
        pword: password of ONDA DIAS page

    Returns
        int: status code of the get request
    '''

    # we use some random url for checking
    url = ('https://catalogue.onda-dias.eu/dias-catalogue/Products(9d3739b5-9210-4f69-80c2-b5362dac10e0)/$value')
    response = requests.get(url, auth=(uname, pword))
    return response.status_code


def s1_download(argument_list):
    '''Function to download a single Sentinel-1 product from ONDA DIAS

    This function will download S1 products from the ONDA DIAS catalogue.

    Args:
        argument_list: a list with 4 entries (this is used to enable parallel
                      execution)
                      argument_list[0] is the product's uuid
                      argument_list[1] is the local path for the download
                      argument_list[2] is the username of Copernicus' scihub
                      argument_list[3] is the password of Copernicus' scihub

    '''
    # get out the arguments
    uuid = argument_list[0]
    #uuid=''.join(uuid)
    filename = argument_list[1]
    uname = argument_list[2]
    pword = argument_list[3]

    # ask for username and password in case you have not defined as input
    if not uname:
        print(' If you do not have a ONDA DIAS user'
              ' account go to: https://www.onda-dias.eu/cms/')
        uname = input(' Your ONDA DIAS Username:')
    if not pword:
        pword = getpass.getpass(' Your ONDA DIAS Password:')

    # define url (url differs from scihub by the lack of '' around the product uuid)
    url = ('https://catalogue.onda-dias.eu/dias-catalogue/'
           'Products({})/$value'.format(uuid))

    # get first response for file Size
    response = requests.get(url, stream=True, auth=(uname, pword))

    # check response
    if response.status_code == 401:
        raise ValueError(' ERROR: Username/Password are incorrect.')
    elif response.status_code != 200:
        print(' ERROR: Something went wrong, will try again in 30 seconds.')
        response.raise_for_status()

    # get download size
    total_length = int(response.headers.get('content-length', 0))

    # define chunk_size
    chunk_size = 1024

    # check if file is partially downloaded
    if os.path.exists(filename):
        first_byte = os.path.getsize(filename)
    else:
        first_byte = 0

    if first_byte >= total_length:
        return total_length

    zip_test = 1
    while zip_test is not None:

        while first_byte < total_length:

            # get byte offset for already downloaded file
            header = {"Range": "bytes={}-{}".format(first_byte, total_length)}

            print(' INFO: Downloading scene to: {}'.format(filename))
            response = requests.get(url, headers=header, stream=True,
                                    auth=(uname, pword))

            # actual download
            with open(filename, "ab") as file:

                if total_length is None:
                    file.write(response.content)
                else:
                    pbar = tqdm.tqdm(total=total_length, initial=first_byte,
                                     unit='B', unit_scale=True,
                                     desc=' INFO: Downloading: ')
                    for chunk in response.iter_content(chunk_size):
                        if chunk:
                            file.write(chunk)
                            pbar.update(chunk_size)
            pbar.close()
            # update first_byte
            first_byte = os.path.getsize(filename)

        # zipFile check
        print(' INFO: Checking the zip archive of {} for inconsistency'.format(
            filename))
        zip_test = h.check_zipfile(filename)
        
        # if it did not pass the test, remove the file
        # in the while loop it will be downlaoded again
        if zip_test is not None:
            print(' INFO: {} did not pass the zip test. \
                  Re-downloading the full scene.'.format(filename))
            os.remove(filename)
            first_byte = 0
        # otherwise we change the status to True
        else:
            print(' INFO: {} passed the zip test.'.format(filename))
            with open(str('{}.downloaded'.format(filename)), 'w') as file:
                file.write('successfully downloaded \n')
            
            
def batch_download(inventory_df, download_dir, uname, pword, concurrent=2):
    
    from ost import Sentinel1_Scene as S1Scene
    from ost.helpers import onda
    
    # create list of scenes
    scenes = inventory_df['identifier'].tolist()
    
    check, i = False, 1
    while check is False and i <= 10:

        download_list = []

        for scene_id in scenes:

            scene = S1Scene(scene_id)
            filepath = scene._download_path(download_dir, True)
            
            try:
                uuid = (inventory_df['uuid']
                    [inventory_df['identifier'] == scene_id].tolist())
            except KeyError:
                uuid = scene.ondadias_uuid(opener=onda.connect(uname=uname,pword=pword)) 
            
            if os.path.exists('{}.downloaded'.format(filepath)):
                print(' INFO: {} is already downloaded.'
                      .format(scene.scene_id))
            else:
                # create list objects for download
                download_list.append([uuid[0], filepath, uname, pword])

        if download_list:
            pool = multiprocessing.Pool(processes=concurrent)
            pool.map(s1_download, download_list)
                    
        downloaded_scenes = glob.glob(
            opj(download_dir, 'SAR', '*', '20*', '*', '*',
                '*.zip.downloaded'))
    
        if len(inventory_df['identifier'].tolist()) == len(downloaded_scenes):
            print(' INFO: All products are downloaded.')
            check = True
        else:
            check = False
            for scene in scenes:

                scene = S1Scene(scene)
                filepath = scene._download_path(download_dir)

                if os.path.exists('{}.downloaded'.format(filepath)):
                    scenes.remove(scene.scene_id)

        i += 1
