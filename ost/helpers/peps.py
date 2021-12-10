#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Functions for connecting and downloading from CNES Peps server
"""

import getpass
import urllib.request
import time
import multiprocessing
import logging
from pathlib import Path

import requests
import tqdm

from ost.helpers import helpers as h

logger = logging.getLogger(__name__)


def ask_credentials():
    """Interactive function asking the user for CNES' Peps credentials

    :return: tuple of username and password
    :rtype: tuple
    """
    # SciHub account details (will be asked by execution)
    print(
        " If you do not have a CNES Peps user account"
        " go to: https://peps.cnes.fr/ and register"
    )
    uname = input(" Your CNES Peps Username:")
    pword = getpass.getpass(" Your CNES Peps Password:")

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
            " If you do not have a CNES Peps user account"
            " go to: https://peps.cnes.fr/ and register"
        )
        uname = input(" Your CNES Peps Username:")

    if not pword:
        pword = getpass.getpass(" Your CNES Peps Password:")

    # open a connection to the CNES Peps
    base_url = "https://peps.cnes.fr/"
    manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    manager.add_password(None, base_url, uname, pword)
    handler = urllib.request.HTTPBasicAuthHandler(manager)
    opener = urllib.request.build_opener(handler)

    return opener


def check_connection(uname, pword):
    """Check if a connection with CNES Pepscan be established

    :param uname:
    :param pword:
    :return:
    """

    response = requests.get(
        "https://peps.cnes.fr/rocket/#/search?view=list&maxRecords=50",
        auth=(uname, pword),
        stream=True,
    )

    return response.status_code


def peps_download(argument_list):
    """Single scene download function for Copernicus scihub/apihub

    :param argument_list:
        a list with 4 entries (this is used to enable parallel execution)
                      argument_list[0]: product's url
                      argument_list[1]: local path for the download
                      argument_list[2]: username of Copernicus' scihub
                      argument_list[3]: password of Copernicus' scihub
    :return:
    """

    url, filename, uname, pword = argument_list
    filename = Path(filename)

    # get first response for file Size
    response = requests.get(url, stream=True, auth=(uname, pword))

    # get download size
    total_length = int(response.headers.get("content-length", 0))

    # define chunk_size
    chunk_size = 1024

    # check if file is partially downloaded
    if filename.exists():

        first_byte = filename.stat().st_size
        if first_byte == total_length:
            logger.info(f"{filename.name} already downloaded.")
        else:
            logger.info(f"Continue downloading scene to: {filename.name}")

    else:
        logger.info(f"Downloading scene to: {filename.resolve()}")
        first_byte = 0

    if first_byte >= total_length:
        return total_length

    zip_test = 1
    while zip_test is not None and zip_test <= 10:

        while first_byte < total_length:

            # get byte offset for already downloaded file
            header = {"Range": f"bytes={first_byte}-{total_length}"}
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
            # updated fileSize
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
            logger.info(f"{filename} passed the zip test.")
            with open(filename.with_suffix(".downloaded"), "w") as file:
                file.write("successfully downloaded \n")


def batch_download(inventory_df, download_dir, uname, pword, concurrent=10):

    from ost import Sentinel1Scene as S1Scene

    logger.info("Getting the storage status (online/onTape) of each scene.")
    logger.info("This may take a while.")

    # this function does not just check,
    # but it already triggers the production of the S1 scene
    inventory_df["pepsStatus"], inventory_df["pepsUrl"] = zip(
        *[
            S1Scene(product).peps_online_status(uname, pword)
            for product in inventory_df.identifier.tolist()
        ]
    )

    # as long as there are any scenes left for downloading, loop
    while len(inventory_df[inventory_df["pepsStatus"] != "downloaded"]) > 0:

        # excluded downlaoded scenes
        inventory_df = inventory_df[inventory_df["pepsStatus"] != "downloaded"]

        # recheck for status
        inventory_df["pepsStatus"], inventory_df["pepsUrl"] = zip(
            *[
                S1Scene(product).peps_online_status(uname, pword)
                for product in inventory_df.identifier.tolist()
            ]
        )

        # if all scenes to download are on Tape, we wait for a minute
        if len(inventory_df[inventory_df["pepsStatus"] == "online"]) == 0:
            logger.info(
                "Imagery still on tape, we will wait for 1 minute " "and try again."
            )
            time.sleep(60)

        # else we start downloading
        else:

            # create the peps_list for parallel download
            peps_list = []
            for index, row in inventory_df[
                inventory_df["pepsStatus"] == "online"
            ].iterrows():

                # get scene identifier
                scene_id = row.identifier
                # construct download path
                scene = S1Scene(scene_id)
                download_path = scene.download_path(download_dir, True)
                # put all info to the peps_list for parallelised download
                peps_list.append(
                    [
                        inventory_df.pepsUrl[
                            inventory_df.identifier == scene_id
                        ].tolist()[0],
                        download_path,
                        uname,
                        pword,
                    ]
                )

            # parallelised download
            pool = multiprocessing.Pool(processes=concurrent)
            pool.map(peps_download, peps_list)

            # routine to check if the file has been downloaded
            for index, row in inventory_df[
                inventory_df["pepsStatus"] == "online"
            ].iterrows():

                # get scene identifier
                scene_id = row.identifier
                # construct download path
                scene = S1Scene(scene_id)
                download_path = scene.download_path(download_dir)
                if download_path.exists():
                    inventory_df.at[index, "pepsStatus"] = "downloaded"
