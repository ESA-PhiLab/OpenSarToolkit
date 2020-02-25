# -*- coding: utf-8 -*-
'''This module provides functions for connecting and downloading
from Alaska satellite Faciltity's Vertex server
'''

import os
from os.path import join as opj
import glob
import requests
import tqdm
import multiprocessing

from ost.helpers import helpers as h
from ost import Sentinel1_Scene as S1Scene


# we need this class for earthdata access
class SessionWithHeaderRedirection(requests.Session):
    ''' A class that helps connect to NASA's Earthdata

    '''

    AUTH_HOST = 'urs.earthdata.nasa.gov'

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    # Overrides from the library to keep headers when redirected to or from
    # the NASA auth host.

    def rebuild_auth(self, prepared_request, response):

        headers = prepared_request.headers
        url = prepared_request.url

        if 'Authorization' in headers:

            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)

            if (original_parsed.hostname != redirect_parsed.hostname) and \
                redirect_parsed.hostname != self.AUTH_HOST and \
                    original_parsed.hostname != self.AUTH_HOST:

                del headers['Authorization']

        return


def check_connection(uname, pword):
    '''A helper function to check if a connection can be established

    Args:
        uname: username of ASF Vertex server
        pword: password of ASF Vertex server

    Returns
        int: status code of the get request
    '''
    url = ('https://datapool.asf.alaska.edu/SLC/SB/S1B_IW_SLC__1SDV_20191119T053342_20191119T053410_018992_023D59_F309.zip')
    #url = ('https://datapool.asf.alaska.edu/SLC/SA/S1A_IW_SLC__1SSV_'
    #       '20160801T234454_20160801T234520_012413_0135F9_B926.zip')
    session = SessionWithHeaderRedirection(uname, pword)
    response = session.get(url, stream=True)
    # print(response)
    return response.status_code


def s1_download(argument_list):
    """
    This function will download S1 products from ASF mirror.

    :param url: the url to the file you want to download
    :param filename: the absolute path to where the downloaded file should
                    be written to
    :param uname: ESA's scihub username
    :param pword: ESA's scihub password
    :return:
    """

    url = argument_list[0]
    filename = argument_list[1]
    uname = argument_list[2]
    pword = argument_list[3]

    session = SessionWithHeaderRedirection(uname, pword)

    print(' INFO: Downloading scene to: {}'.format(filename))
    # submit the request using the session
    response = session.get(url, stream=True)

    # raise an exception in case of http errors
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

    zip_test = 1
    while zip_test is not None and zip_test <= 10:

        while first_byte < total_length:

            # get byte offset for already downloaded file
            header = {"Range": "bytes={}-{}".format(first_byte, total_length)}
            response = session.get(url, headers=header, stream=True)

            # actual download
            with open(filename, "ab") as file:

                if total_length is None:
                    file.write(response.content)
                else:
                    pbar = tqdm.tqdm(total=total_length, initial=first_byte,
                                     unit='B', unit_scale=True,
                                     desc=' INFO: Downloading ')

                    for chunk in response.iter_content(chunk_size):
                        if chunk:
                            file.write(chunk)
                            pbar.update(chunk_size)

            pbar.close()

            # updated fileSize
            first_byte = os.path.getsize(filename)

        print(' INFO: Checking the zip archive of {} for inconsistency'
                  .format(filename))
        zip_test = h.check_zipfile(filename)
        # if it did not pass the test, remove the file
        # in the while loop it will be downlaoded again
        if zip_test is not None:
            print(' INFO: {} did not pass the zip test. \
                  Re-downloading the full scene.'.format(filename))
            if os.path.exists(filename):
                os.remove(filename)
                first_byte = 0
            # otherwise we change the status to True
        else:
            print(' INFO: {} passed the zip test.'.format(filename))
            with open(str('{}.downloaded'.format(filename)), 'w') as file:
                file.write('successfully downloaded \n')


def batch_download(inventory_df, download_dir, uname, pword, concurrent=10):

    # create list of scenes
    scenes = inventory_df['identifier'].tolist()
    
    check, i = False, 1
    while check is False and i <= 10:

        asf_list = []

        for scene_id in scenes:

            scene = S1Scene(scene_id)
            filepath = scene._download_path(download_dir, True)

            if os.path.exists('{}.downloaded'.format(filepath)):
                print(' INFO: {} is already downloaded.'
                      .format(scene.scene_id))
            else:
                asf_list.append([scene.asf_url(), filepath,
                                 uname, pword])

        if asf_list:
            pool = multiprocessing.Pool(processes=concurrent)
            pool.map(s1_download, asf_list)
                    
        downloaded_scenes = glob.glob(
            opj(download_dir, 'SAR', '*', '20*', '*', '*',
                '*.zip.downloaded'))

        if len(inventory_df['identifier'].tolist()) == len(downloaded_scenes):
            check = True
            print(' INFO: All products are downloaded.')
        else:
            check = False
            for scene in scenes:

                scene = S1Scene(scene)
                filepath = scene._download_path(download_dir)

                if os.path.exists('{}.downloaded'.format(filepath)):
                    scenes.remove(scene.scene_id)

        i += 1
