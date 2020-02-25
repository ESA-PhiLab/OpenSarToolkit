# -*- coding: utf-8 -*-
'''
This module provides functions for searching and downloading data from
Copernicus scihub server.
'''

import os
from os.path import join as opj
import glob
import getpass
import datetime
import multiprocessing
import urllib
import requests
import tqdm
#import zipfile
from shapely.wkt import loads

from ost.helpers import helpers as h


def ask_credentials():
    '''A helper function that asks for user credentials on Copernicus' Scihub

    Returns:
        uname: username of Copernicus' scihub  page
        pword: password of Copernicus' scihub  page

    '''
    # SciHub account details (will be asked by execution)
    print(' If you do not have a Copernicus Scihub user account'
          ' go to: https://scihub.copernicus.eu and register')
    uname = input(' Your Copernicus Scihub Username:')
    pword = getpass.getpass(' Your Copernicus Scihub Password:')

    return uname, pword


def connect(base_url='https://scihub.copernicus.eu/apihub/',
            uname=None, pword=None):
    '''A helper function to connect and authenticate to the Copernicus' scihub.

    Args:
        base_url (str): basic url to the Copernicus' scihub
        uname (str): username of Copernicus' scihub
        pword (str): password of Copernicus' scihub

    Returns:
        opener: an urllib opener instance ot Copernicus' scihub

    '''

    if not uname:
        print(' If you do not have a Copernicus Scihub user'
              ' account go to: https://scihub.copernicus.eu')
        uname = input(' Your Copernicus Scihub Username:')

    if not pword:
        pword = getpass.getpass(' Your Copernicus Scihub Password:')

    # open a connection to the scihub
    manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    manager.add_password(None, base_url, uname, pword)
    handler = urllib.request.HTTPBasicAuthHandler(manager)
    opener = urllib.request.build_opener(handler)

    return opener


def next_page(dom):
    '''A helper function to iterate over the search results pages.

    Args:
        dom: an xml.dom object coming back from a Copernicus' scihub request

    Returns:
        str: Link ot the next page or None if we reached the end.

    '''

    links = dom.getElementsByTagName('link')
    next_page_, this_page, last = None, None, None

    for link in links:
        if link.getAttribute('rel') == 'next':
            next_page_ = link.getAttribute('href')
        elif link.getAttribute('rel') == 'self':
            this_page = link.getAttribute('href')
        elif link.getAttribute('rel') == 'last':
            last = link.getAttribute('href')

    if last == this_page:   # we are not at the end
        next_page_ = None

    return next_page_


def create_satellite_string(sat):
    '''A helper function to create the full Satellite string

    Args:
        sat (str): is a OST scene mission_id attribute (e.g. S1)

    Returns:
        str: Full name of the satellite

    '''

    if str(1) in sat:
        sat = 'Sentinel-1'
    elif str(2) in sat:
        sat = 'Sentinel-2'
    elif str(3) in sat:
        sat = 'Sentinel-3'
    elif str(5) in sat:
        sat = 'Sentinel-5'

    return 'platformname:{}'.format(sat)


def create_aoi_str(aoi_wkt):
    '''A helper function to create a scihub API compliant AOI string

    Args:
        aoi (str): is WKT representation of the Area Of Interest

    Returns:
        str: Copernicus' scihub compliant AOI string

    '''

    geom = loads(aoi_wkt)
    if geom.geom_type == 'Point':
        aoi_str = "( footprint:\"Intersects({}, {})\")".format(geom.y, geom.x)

    else:
        # simplify geometry
        aoi_convex = geom.convex_hull

        # create scihub-confrom aoi string
        aoi_str = '( footprint:\"Intersects({})\")'.format(aoi_convex)

    return aoi_str


def create_toi_str(start='2014-10-01',
                   end=datetime.datetime.now().strftime("%Y-%m-%d")):
    '''A helper function to create a scihub API compliant TOI string

    Args:
        start (str): the start date of the Time Of Interest represented by
                     a string of a YYYY-MM-DD format string
        end (str): the end date of the Time Of Interest represented by
                   a string of a YYYY-MM-DD format string

    Returns:
        str: Copernicus' scihub compliant TOI string

    '''

    # bring start and end date to query format
    start = '{}T00:00:00.000Z'.format(start)
    end = '{}T23:59:59.999Z'.format(end)
    toi = ('beginPosition:[{} TO {}] AND '
           'endPosition:[{} TO {}]'.format(start, end,
                                           start, end))

    return toi


def create_s1_product_specs(product_type='*', polarisation='*', beam='*'):
    '''A helper function to create a scihub API compliant product specs string

    Args:
        product_type (str): the product type to look at
        polarisation (str):

    Returns:
        str: Copernicus' scihub compliant product specs string

    Notes:
        Default values for all product specifications is the *-character,
        meaning to look for all kinds of data by default.

    '''

    # bring product type, polarisation and beam to query format
    product_type = "producttype:{}".format(product_type)
    polarisation = "polarisationMode:{}".format(polarisation)
    beam = "sensoroperationalmode:{}".format(beam)

    return '{} AND {} AND {}'.format(product_type, polarisation, beam)


def create_query(satellite, aoi, toi, product_specs):
    '''A helper function to create a scihub API compliant query

    Args:
        satellite (str): the satellite (e.g. Sentinel-1)
        aoi (str): a Copernicus scihub compliant AOI string
        toi (str): a Copernicus scihub compliant TOI string
        product_specs (str): a Copernicus scihub compliant product specs string

    Returns:
        str: Copernicus' scihub compliant query string (i.e. OpenSearch query)
             formattted with urllib

    '''
    # construct the final query
    query = urllib.request.quote('{} AND {} AND {} AND {}'.format(
        satellite, product_specs, aoi, toi))

    return query


def check_connection(uname, pword):
    '''A helper function to check if a connection  can be established

    Args:
        uname: username of Copernicus' scihub page
        pword: password of Copernicus' scihub page

    Returns
        int: status code of the get request
    '''

    # we use some random url for checking
    url = ('https://scihub.copernicus.eu/apihub/odata/v1/Products?'
           '$select=Id&$filter=substringof(%27_20171113T010515_%27,Name)')
    response = requests.get(url, auth=(uname, pword))
    return response.status_code


def s1_download(argument_list):
    '''Function to download a single Sentinel-1 product from Copernicus scihub

    This function will download S1 products from ESA's apihub.

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
    filename = argument_list[1]
    uname = argument_list[2]
    pword = argument_list[3]

    # ask for username and password in case you have not defined as input
    if not uname:
        print(' If you do not have a Copernicus Scihub user'
              ' account go to: https://scihub.copernicus.eu')
        uname = input(' Your Copernicus Scihub Username:')
    if not pword:
        pword = getpass.getpass(' Your Copernicus Scihub Password:')

    # define url
    url = ('https://scihub.copernicus.eu/apihub/odata/v1/'
           'Products(\'{}\')/$value'.format(uuid))

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
    from ost.helpers import scihub
    
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
                uuid = scene.scihub_uuid(scihub.connect(uname, pword)) 
            
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

            