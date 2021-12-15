#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Functions for downloading from ONDA Dias mirror.
"""

import getpass
import multiprocessing
import urllib.request
import requests
import tqdm
import logging
from pathlib import Path

from ost.helpers import helpers as h

logger = logging.getLogger(__name__)


def ask_credentials():
    """Interactive function asking the user for ONDA Dias credentials

    :return: tuple of username and password
    :rtype: tuple
    """

    # SciHub account details (will be asked by execution)
    print(
        " If you do not have a ONDA DIAS user account"
        " go to: https://www.onda-dias.eu/cms/ and register"
    )
    uname = input(" Your ONDA DIAS Username:")
    pword = getpass.getpass(" Your ONDA DIAS Password:")

    return uname, pword


def connect(uname=None, pword=None):
    """Generates an opener for the Copernicus apihub/dhus

    :param uname: username of ONDA Dias
    :type uname: str
    :param pword: password of ONDA Dias
    :type pword: str
    :return: an urllib opener instance for Copernicus' scihub
    :rtype: opener object
    """

    if not uname:
        print(
            " If you do not have a ONDA DIAS user"
            " account go to: https://www.onda-dias.eu/cms/"
        )
        uname = input(" Your ONDA DIAS Username:")

    if not pword:
        pword = getpass.getpass(" Your ONDA DIAS Password:")

    base_url = "https://catalogue.onda-dias.eu/dias-catalogue/"

    # open a connection to the scihub
    manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    manager.add_password(None, base_url, uname, pword)
    handler = urllib.request.HTTPBasicAuthHandler(manager)
    opener = urllib.request.build_opener(handler)

    return opener


def check_connection(uname, pword):
    """Check if a connection with ONDA Dias can be established

    :param uname:
    :param pword:
    :return:
    """

    # we use some random url for checking
    url = (
        "https://catalogue.onda-dias.eu/dias-catalogue/"
        "Products(6e0e382e-45f2-4969-96b0-1130b6cd2fa6)/$value"
    )
    response = requests.get(url, auth=(uname, pword), stream=True)
    return response.status_code


def onda_download(argument_list):
    """Single scene download function for Copernicus scihub/apihub

    :param argument_list:
        a list with 4 entries (this is used to enable parallel execution)
                      argument_list[0]: product's uuid
                      argument_list[1]: local path for the download
                      argument_list[2]: username of ONDA Dias
                      argument_list[3]: password of ONDA Dias
    :return:
    """

    # get out the arguments
    uuid, filename, uname, pword = argument_list
    filename = Path(filename)

    # ask for username and password in case you have not defined as input
    if not uname:
        print(
            " If you do not have a ONDA DIAS user"
            " account go to: https://www.onda-dias.eu/cms/"
        )
        uname = input(" Your ONDA DIAS Username:")

    if not pword:
        pword = getpass.getpass(" Your ONDA DIAS Password:")

    # define url (url differs from scihub by the
    # lack of '' around the product uuid)
    url = f"https://catalogue.onda-dias.eu/dias-catalogue/" f"Products({uuid})/$value"

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
    if filename.exists():
        first_byte = filename.stat().st_size
    else:
        first_byte = 0

    if first_byte >= total_length:
        return total_length

    zip_test = 1
    while zip_test is not None:

        while first_byte < total_length:

            # get byte offset for already downloaded file
            header = {"Range": f"bytes={first_byte}-{total_length}"}

            logger.info(f"Downloading scene to: {filename.resolve()}")
            response = requests.get(
                url, headers=header, stream=True, auth=(uname, pword)
            )

            # actual download
            with open(filename, "ab") as file:

                if total_length is None:
                    file.write(response.content)
                else:
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
        logger.info(f"Checking the zip archive of {filename.name} for inconsistency")
        zip_test = h.check_zipfile(filename)

        # if it did not pass the test, remove the file
        # in the while loop it will be downlaoded again
        if zip_test is not None:
            logger.info(
                f"{filename.name} did not pass the zip test. "
                f"Re-downloading the full scene."
            )
            filename.unlink()
            first_byte = 0
        # otherwise we change the status to True
        else:
            logger.info(f"{filename.name} passed the zip test.")
            with open(filename.with_suffix(".downloaded"), "w") as file:
                file.write("successfully downloaded \n")


def batch_download(inventory_df, download_dir, uname, pword, concurrent=2):

    from ost import Sentinel1Scene as S1Scene
    from ost.helpers import onda

    # create list of scenes
    scenes = inventory_df["identifier"].tolist()

    check, i = False, 1
    while check is False and i <= 10:

        download_list = []

        for scene_id in scenes:

            scene = S1Scene(scene_id)
            file_path = scene.download_path(download_dir, True)

            try:
                uuid = inventory_df["uuid"][
                    inventory_df["identifier"] == scene_id
                ].tolist()
            except KeyError:
                uuid = scene.ondadias_uuid(
                    opener=onda.connect(uname=uname, pword=pword)
                )

            if file_path.with_suffix(".downloaded").exists():
                logger.info(f"{scene.scene_id} is already downloaded.")
            else:
                # create list objects for download
                download_list.append([uuid[0], file_path, uname, pword])

        if download_list:
            pool = multiprocessing.Pool(processes=concurrent)
            pool.map(onda_download, download_list)

        downloaded_scenes = list(download_dir.glob("**/*.downloaded"))

        if len(inventory_df["identifier"].tolist()) == len(downloaded_scenes):
            logger.info("All products are downloaded.")
            check = True
        else:
            check = False
            for scene in scenes:

                scene = S1Scene(scene)
                file_path = scene.download_path(download_dir)

                if file_path.with_suffix(".downloaded"):
                    scenes.remove(scene.scene_id)

        i += 1
