# -*- coding: utf-8 -*-
'''This module provides functions for connecting and downloading
from Alaska satellite Faciltity's Vertex server
'''

import os
import requests
import tqdm


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

    url = ('https://datapool.asf.alaska.edu/SLC/SA/S1A_IW_SLC__1SSV_'
           '20160801T234454_20160801T234520_012413_0135F9_B926.zip')
    session = SessionWithHeaderRedirection(uname, pword)
    response = session.get(url, stream=True)
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

        # if first_byte >= total_length:
        #    downloaded = True

        # except requests.exceptions.HTTPError as e:
        #    downloaded = False
        # handle any errors here
        #    print(e)
