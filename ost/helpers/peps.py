# -*- coding: utf-8 -*-

# import standard libs
import os
import getpass
import urllib
import time
import multiprocessing

# import non-standar libes
import requests
import tqdm

# import ost classes/functions
from ost.helpers import helpers as h


def ask_credentials():
    '''A helper function that asks for user credentials on CNES' PEPS

    Returns:
        uname (str): username of CNES' PEPS page
        pword (str): password of CNES' PEPS page

    '''
    # SciHub account details (will be asked by execution)
    print(' If you do not have a CNES Peps user account'
          ' go to: https://peps.cnes.fr/ and register')
    uname = input(' Your CNES Peps Username:')
    pword = getpass.getpass(' Your CNES Peps Password:')

    return uname, pword


def connect(uname=None, pword=None):
    '''A helper function to connect and authenticate to CNES' PEPS.

    Args:
        uname (str): username of CNES' PEPS page
        pword (str): password of CNES' PEPS page

    Returns:
        opener: an urllib opener instance at CNES' PEPS

    '''

    base_url = 'https://peps.cnes.fr/'

    if not uname:
        print(' If you do not have a CNES Peps user account'
              ' go to: https://peps.cnes.fr/ and register')
        uname = input(' Your CNES Peps Username:')

    if not pword:
        pword = getpass.getpass(' Your CNES Peps Password:')

    # open a connection to the scihub
    manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    manager.add_password(None, base_url, uname, pword)
    handler = urllib.request.HTTPBasicAuthHandler(manager)
    opener = urllib.request.build_opener(handler)

    return opener


def check_connection(uname, pword):
    '''A helper function to check if a connection can be established

    Args:
        uname: username of CNES' PEPS
        pword: password of CNES' PEPS

    Returns
        int: status code of the get request
    '''

    response = requests.get(
        'https://peps.cnes.fr/rocket/#/search?view=list&maxRecords=50',
        auth=(uname, pword))

    return response.status_code


def s1_download(argument_list):
    '''Function to download a single Sentinel-1 product from CNES' PEPS

    Args:
        argument_list: a list with 4 entries (this is used to enable parallel
                       execution)
                       argument_list[0] is the product's url
                       argument_list[1] is the local path for the download
                       argument_list[2] is the username of CNES' PEPS
                       argument_list[3] is the password of CNES' PEPS

    '''

    url = argument_list[0]
    filename = argument_list[1]
    uname = argument_list[2]
    pword = argument_list[3]

    # get first response for file Size
    response = requests.get(url, stream=True, auth=(uname, pword))

    # get download size
    total_length = int(response.headers.get('content-length', 0))

    # define chunk_size
    chunk_size = 1024

    # check if file is partially downloaded
    if os.path.exists(filename):

        first_byte = os.path.getsize(filename)
        if first_byte == total_length:
            print(' INFO: {} already downloaded.'.format(filename))
        else:
            print(' INFO: Continue downloading scene to: {}'.format(
                filename))

    else:
        print(' INFO: Downloading scene to: {}'.format(filename))
        first_byte = 0

    if first_byte >= total_length:
        return total_length

    zip_test = 1
    while zip_test is not None and zip_test <= 10:

        while first_byte < total_length:
            
            # get byte offset for already downloaded file
            header = {"Range": "bytes={}-{}".format(first_byte, total_length)}
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
            # updated fileSize
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


def batch_download(inventory_df, download_dir, uname, pword, concurrent=10):

    from ost import Sentinel1_Scene as S1Scene
    print(' INFO: Getting the storage status (online/onTape) of each scene.')
    print(' INFO: This may take a while.')

    # this function does not just check,
    # but it already triggers the production of the S1 scene
    inventory_df['pepsStatus'], inventory_df['pepsUrl'] = (
        zip(*[S1Scene(product).peps_online_status(uname, pword)
              for product in inventory_df.identifier.tolist()]))

    # as long as there are any scenes left for downloading, loop
    while len(inventory_df[inventory_df['pepsStatus'] != 'downloaded']) > 0:

        # excluded downlaoded scenes
        inventory_df = inventory_df[inventory_df['pepsStatus'] != 'downloaded']

        # recheck for status
        inventory_df['pepsStatus'], inventory_df['pepsUrl'] = (
            zip(*[S1Scene(product).peps_online_status(uname, pword)
                  for product in inventory_df.identifier.tolist()]))

        # if all scenes to download are on Tape, we wait for a minute
        if len(inventory_df[inventory_df['pepsStatus'] == 'online']) == 0:
            print('INFO: Imagery still on tape, we will wait for 1 minute ' \
                  'and try again.')
            time.sleep(60)

        # else we start downloading
        else:

            # create the peps_list for parallel download
            peps_list = []
            for index, row in (
                    inventory_df[inventory_df['pepsStatus'] == 'online']
                    .iterrows()):

                # get scene identifier
                scene_id = row.identifier
                # construct download path
                scene = S1Scene(scene_id)
                download_path = scene._download_path(download_dir, True)
                # put all info to the peps_list for parallelised download
                peps_list.append(
                    [inventory_df.pepsUrl[
                        inventory_df.identifier == scene_id].tolist()[0],
                        download_path, uname, pword])

            # parallelised download
            pool = multiprocessing.Pool(processes=concurrent)
            pool.map(s1_download, peps_list)

            # routine to check if the file has been downloaded
            for index, row in (
                    inventory_df[inventory_df['pepsStatus'] == 'online']
                    .iterrows()):

                # get scene identifier
                scene_id = row.identifier
                # construct download path
                scene = S1Scene(scene_id)
                download_path = scene._download_path(download_dir)
                if os.path.exists(download_path):
                    inventory_df.at[index, 'pepsStatus'] = 'downloaded'
