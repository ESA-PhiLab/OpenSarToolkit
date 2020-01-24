import os
from os.path import join as opj
import glob
import logging
import requests
import multiprocessing
from http.cookiejar import CookieJar
import urllib.error
import urllib.request as urlreq


from ost.helpers import helpers as h
from ost.helpers.helpers import TqdmUpTo
from ost import Sentinel1Scene as S1Scene

logger = logging.getLogger(__name__)


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
    password_manager = urlreq.HTTPPasswordMgrWithDefaultRealm()
    password_manager.add_password(
        None, "https://urs.earthdata.nasa.gov", uname, pword
    )

    cookie_jar = CookieJar()

    opener = urlreq.build_opener(
        urlreq.HTTPBasicAuthHandler(password_manager),
        urlreq.HTTPCookieProcessor(cookie_jar)
    )
    urlreq.install_opener(opener)

    url = ('https://datapool.asf.alaska.edu/SLC/SA/S1A_IW_SLC__1SSV_'
           '20160801T234454_20160801T234520_012413_0135F9_B926.zip'
           )

    try:
        urlreq.urlopen(url=url)
    except urllib.error.HTTPError as e:
        # Return code error (e.g. 404, 501, ...)
        # ...
        response_code = e.reason
    except urllib.error.URLError as e:
        # Not an HTTP-specific error (e.g. connection refused)
        # ...
        response_code = e.reason
    else:
        response_code = 200
    return response_code


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

    password_manager = urlreq.HTTPPasswordMgrWithDefaultRealm()
    password_manager.add_password(
        None, "https://urs.earthdata.nasa.gov", uname, pword
    )

    cookie_jar = CookieJar()

    opener = urlreq.build_opener(
        urlreq.HTTPBasicAuthHandler(password_manager),
        urlreq.HTTPCookieProcessor(cookie_jar)
    )
    urlreq.install_opener(opener)

    logger.debug('INFO: Downloading scene to: {}'.format(filename))
    # submit the request using the session
    try:
        response = urlreq.urlopen(url=url)
    # raise an exception in case of http errors
    except urllib.error.HTTPError as e:
        # Raise error (e.g. 404, 501, ...)
        # ...
        raise e
    except urllib.error.URLError as e:
        # Not an HTTP-specific error (e.g. connection refused)
        # ...
        raise e

    # get download size
    total_length = int(response.headers.get('content-length', 0))

    # check if file is partially downloaded
    if os.path.exists(filename):
        first_byte = os.path.getsize(filename)
    else:
        first_byte = 0
    if first_byte == total_length:
        first_byte = total_length
    elif first_byte > 0 and first_byte < total_length:
        os.remove(filename)
    zip_test = 1
    while zip_test is not None and zip_test <= 10:
        while first_byte < total_length:
            with TqdmUpTo(unit='B', unit_scale=True, miniters=1,
                          desc=url.split('/')[-1]) as t:
                filename, headers = urlreq.urlretrieve(
                    url, filename=filename, reporthook=t.update_to
                )
            # updated fileSize
            first_byte = os.path.getsize(filename)
        logger.debug('INFO: Checking the zip archive of {} for inconsistency'
                     .format(filename))
        zip_test = h.check_zipfile(filename)
        # if it did not pass the test, remove the file
        # in the while loop it will be downlaoded again
        if zip_test is not None:
            logger.debug('INFO: {} did not pass the zip test. \
                  Re-downloading the full scene.'.format(filename))
            if os.path.exists(filename):
                os.remove(filename)
                first_byte = 0
            # otherwise we change the status to True
        else:
            logger.debug('INFO: {} passed the zip test.'.format(filename))
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
                logger.debug('INFO: {} is already downloaded.'
                             .format(scene.scene_id))
            else:
                asf_list.append([scene.asf_url(), filepath,
                                 uname, pword])
        if asf_list:
            pool = multiprocessing.Pool(processes=concurrent)
            pool.map(s1_download, asf_list)
                    
        downloaded_scenes = glob.glob(
            opj(download_dir, 'SAR', '*', '20*', '*', '*',
                '*.zip.downloaded')
        )

        if len(inventory_df['identifier'].tolist()) == len(downloaded_scenes):
            check = True
            logger.debug('INFO: All products are downloaded.')
        else:
            check = False
            for scene in scenes:

                scene = S1Scene(scene)
                filepath = scene._download_path(download_dir)

                if os.path.exists('{}.downloaded'.format(filepath)):
                    scenes.remove(scene.scene_id)

        i += 1
