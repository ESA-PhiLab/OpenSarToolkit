#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""Functions for searching and downloading from Copernicus scihub.
"""

import getpass
import datetime
import logging
import multiprocessing
import urllib.request
import requests
from pathlib import Path

import tqdm
from shapely.wkt import loads

from ost.helpers import helpers as h

logger = logging.getLogger(__name__)


def ask_credentials():
    """Interactive function asking the user for scihub credentials

    :return: tuple of username and password
    :rtype: tuple
    """

    print(
        " If you do not have a Copernicus Scihub user account"
        " go to: https://scihub.copernicus.eu and register"
    )
    uname = input(" Your Copernicus Scihub Username:")
    pword = getpass.getpass(" Your Copernicus Scihub Password:")

    return uname, pword


def connect(uname=None, pword=None, base_url="https://apihub.copernicus.eu/apihub"):
    """Generates an opener for the Copernicus apihub/dhus


    :param uname: username of Copernicus' scihub
    :type uname: str
    :param pword: password of Copernicus' scihub
    :type pword: str
    :param base_url:
    :return: an urllib opener instance for Copernicus' scihub
    :rtype: opener object
    """

    if not uname:
        print(
            " If you do not have a Copernicus Scihub user"
            " account go to: https://scihub.copernicus.eu"
        )
        uname = input(" Your Copernicus Scihub Username:")

    if not pword:
        pword = getpass.getpass(" Your Copernicus Scihub Password:")

    # create opener
    manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    manager.add_password(None, base_url, uname, pword)
    handler = urllib.request.HTTPBasicAuthHandler(manager)
    opener = urllib.request.build_opener(handler)

    return opener


def next_page(dom):
    """Gets link for next page for results from apihub/scihub

    :param dom: object coming back from a Copernicus' scihub search request
    :type dom: xml.dom object
    :return: Link ot the next page or None if we reached the end.
    :rtype: str
    """

    links = dom.getElementsByTagName("link")
    next_page_, this_page, last = None, None, None

    for link in links:
        if link.getAttribute("rel") == "next":
            next_page_ = link.getAttribute("href")
        elif link.getAttribute("rel") == "self":
            this_page = link.getAttribute("href")
        elif link.getAttribute("rel") == "last":
            last = link.getAttribute("href")

    if last == this_page:  # we are not at the end
        next_page_ = None

    return next_page_


def create_satellite_string(mission_id):
    """Convert mission_id to scihub's search url platformname attribute

    :param mission_id: an OST scene mission_id attribute (e.g. S1)
    :return: Copernicus' scihub compliant satellite query string
    :rtype: str
    """

    if str(1) in mission_id:
        return "platformname:Sentinel-1"
    elif str(2) in mission_id:
        return "platformname:Sentinel-2"
    elif str(3) in mission_id:
        return "platformname:Sentinel-3"
    elif str(5) in mission_id:
        return "platformname:Sentinel-5"
    else:
        raise ValueError("No satellite with mission_id")


def create_aoi_str(aoi):
    """Convert WKT formatted AOI to scihub's search url footprint attribute

    :param aoi: WKT representation of the Area Of Interest
    :type aoi: WKT string
    :return: Copernicus' scihub compliant AOI query string
    :rtype: str
    """

    # load to shapely geometry to easily test for geometry type
    geom = loads(aoi)

    # dependent on the type construct the query string
    if geom.geom_type == "Point":
        return f'( footprint:"Intersects({geom.y}, {geom.x})")'

    else:
        # simplify geometry
        aoi_convex = geom.convex_hull

        # create scihub-confrom aoi string
        return f'( footprint:"Intersects({aoi_convex})")'


def create_toi_str(
    start="2014-10-01", end=datetime.datetime.now().strftime("%Y-%m-%d")
):
    """Convert start and end date to scihub's search url time period attribute

    :param start: start date as a YYYY-MM-DD formatted string,
                  defaults to '2014-10-01'
    :type start: string, YYYY-MM-DD date format
    :param end: end date as a YYYY-MM-DD formatted string,
                defaults to now
    :type end: string, YYYY-MM-DD date format
    :return: Copernicus' scihub compliant TOI query string
    :rtype: str
    """

    # bring start and end date to query format
    start = f"{start}T00:00:00.000Z"
    end = f"{end}T23:59:59.999Z"
    return f"beginPosition:[{start} TO {end}] AND endPosition:[{start} TO {end}]"


def create_s1_product_specs(product_type="*", polarisation="*", beam="*"):
    """Convert Sentinel-1's product metadata to scihub's product attributes

    Default values for all product specifications is the wildcard
    '*' in order to check for all

    :param product_type: Sentinel-1 product type (RAW, SLC, GRD),
                         defaults to '*'
    :type product_type: str
    :param polarisation: Sentinel-1 polarisation mode (VV; VV VH; HH; HH HV),
                         defaults to '*'
    :type polarisation: string
    :param beam: Sentinel-1 beam mode (IW; SM, EW), defaults to '*'
    :type beam: str
    :return: Copernicus' scihub compliant product specifications query string
    :rtype: str
    """

    # bring product type, polarisation and beam to query format
    return (
        f"producttype:{product_type} AND "
        f"polarisationMode:{polarisation} AND "
        f"sensoroperationalmode:{beam}"
    )


def check_connection(uname, pword, base_url="https://apihub.copernicus.eu/apihub"):
    """Check if a connection with scihub can be established

    :param uname:
    :param pword:
    :param base_url:
    :return:
    """

    # we use some random url for checking (also for czech mirror)
    url = (
        f"{base_url}/odata/v1/Products("
        "'8f30a536-c01c-4ef4-ac74-be3378dc44c4')/$value"
    )

    response = requests.get(url, auth=(uname, pword), stream=True)
    return response.status_code


def s1_download_parallel(argument_list):
    """Helper function for parallel download from scihub"""

    uuid, filename, uname, pword, base_url = argument_list
    s1_download(uuid, filename, uname, pword, base_url)


def s1_download(
    uuid, filename, uname, pword, base_url="https://apihub.copernicus.eu/apihub"
):
    """Single scene download function for Copernicus scihub/apihub

    :param uuid: product's uuid
    :param filename: local path for the download
    :param uname: username of Copernicus' scihub
    :param pword: password of Copernicus' scihub
    :param base_url:

    :return:
    """

    # get out the arguments
    if isinstance(filename, str):
        filename = Path(filename)

    # ask for credentials in case they are not defined as input
    if not uname or not pword:
        ask_credentials()

    # define url
    url = f"{base_url}/odata/v1/Products('{uuid}')/$value"

    # get first response for file Size
    response = requests.get(url, stream=True, auth=(uname, pword))

    # check response
    if response.status_code == 401:
        raise ValueError(" ERROR: Username/Password are incorrect.")
    elif response.status_code != 200:
        print(" ERROR: Something went wrong, will try again in 30 seconds.")
        response.raise_for_status()

    # get download size
    total_length = int(response.headers.get("content-length", 0))

    # define chunk_size
    chunk_size = 1024

    # check if file is partially downloaded
    first_byte = filename.stat().st_size if filename.exists() else 0

    if first_byte >= total_length:
        return

    zip_test = 1
    while zip_test is not None:

        while first_byte < total_length:

            # get byte offset for already downloaded file
            header = {"Range": f"bytes={first_byte}-{total_length}"}

            logger.info(f"Downloading scene to: {filename.name}")
            response = requests.get(
                url, headers=header, stream=True, auth=(uname, pword)
            )

            # actual download
            with open(filename, "ab") as file:

                pbar = tqdm.tqdm(
                    total=total_length,
                    initial=first_byte,
                    unit="B",
                    unit_scale=True,
                    desc=" INFO: Downloading: ",
                )
                for chunk in response.iter_content(chunk_size):
                    if chunk:
                        file.write(chunk)
                        pbar.update(chunk_size)
            pbar.close()

            # update first_byte
            first_byte = filename.stat().st_size

        # zipFile check
        logger.info(f"Checking zip archive {filename.name} for inconsistency")
        zip_test = h.check_zipfile(filename)

        # if it did not pass the test, remove the file
        # in the while loop it will be downloaded again
        if zip_test is not None:
            logger.info(
                f"{filename.name} did not pass the zip test. Re-downloading "
                f"the full scene."
            )
            filename.unlink()
            first_byte = 0
        # otherwise we change the status to True
        else:
            logger.info(f"{filename.name} passed the zip test.")
            with open(filename.with_suffix(".downloaded"), "w") as file:
                file.write("successfully downloaded \n")


def batch_download(
    inventory_df,
    download_dir,
    uname,
    pword,
    concurrent=2,
    base_url="https://apihub.copernicus.eu/apihub",
):
    """Batch download Sentinel-1 on the basis of an OST inventory GeoDataFrame

    :param inventory_df:
    :param download_dir:
    :param uname:
    :param pword:
    :param concurrent:
    :param base_url:

    :return:
    """
    from ost import Sentinel1Scene as S1Scene

    if isinstance(download_dir, str):
        download_dir = Path(download_dir)

    # create list of scenes
    scenes = inventory_df["identifier"].tolist()

    check, i = False, 1
    while check is False and i <= 10:

        download_list = []
        for scene_id in scenes:
            scene = S1Scene(scene_id)
            filepath = scene.download_path(download_dir, True)

            try:
                uuid = inventory_df["uuid"][
                    inventory_df["identifier"] == scene_id
                ].tolist()
            except KeyError:
                uuid = [
                    scene.scihub_uuid(
                        connect(uname=uname, pword=pword, base_url=base_url)
                    )
                ]

            if Path(f"{filepath}.downloaded").exists():
                logger.debug(f"{scene.scene_id} is already downloaded.")
            else:
                # create list objects for download
                download_list.append([uuid[0], filepath, uname, pword, base_url])

        if download_list:
            pool = multiprocessing.Pool(processes=concurrent)
            pool.map(s1_download_parallel, download_list)

        downloaded_scenes = list(download_dir.glob("**/*.downloaded"))

        if len(inventory_df["identifier"].tolist()) == len(downloaded_scenes):
            logger.info("All products are downloaded.")
            check = True
        else:
            check = False
            for scene in scenes:

                scene = S1Scene(scene)
                file_path = scene.download_path(download_dir)

                if file_path.with_suffix(".downloaded").exists():
                    scenes.remove(scene.scene_id)

        i += 1
