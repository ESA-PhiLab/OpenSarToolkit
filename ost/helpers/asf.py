#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Functions for connecting and downloading from Alaska Satellite Facility
"""

import getpass
import logging
import requests
from pathlib import Path

import tqdm
from retrying import retry
from godale import Executor

from ost.helpers import helpers as h
from ost.helpers.errors import DownloadError

logger = logging.getLogger(__name__)


def ask_credentials():
    """Interactive function asking the user for ASF credentials

    :return: tuple of username and password
    :rtype: tuple
    """

    # SciHub account details (will be asked by execution)
    print(
        " If you do not have a ASF/NASA Earthdata user account"
        " go to: https://search.asf.alaska.edu/ and register"
    )
    uname = input(" Your ASF/NASA Earthdata Username:")
    pword = getpass.getpass(" Your ASF/NASA Earthdata Password:")

    return uname, pword


def check_connection(uname, pword):
    """Check if a connection with scihub can be established

    :param uname:
    :param pword:
    :return:
    """

    # random url to check
    url = (
        "https://datapool.asf.alaska.edu/SLC/SA/S1A_IW_SLC__1SSV_"
        "20160801T234454_20160801T234520_012413_0135F9_B926.zip"
    )

    with requests.Session() as session:
        session.auth = (uname, pword)
        request = session.request("get", url)
        response = session.get(request.url, auth=(uname, pword), stream=True)
        return response.status_code


def asf_download_parallel(argument_list):

    url, filename, uname, pword = argument_list
    asf_download(url, filename, uname, pword)


@retry(stop_max_attempt_number=5, wait_fixed=5)
def asf_download(url, filename, uname, pword):
    """
    This function will download S1 products from ASF mirror.
    :param url: the url to the file you want to download
    :param filename: the absolute path to where the downloaded file should
                    be written to
    :param uname: ESA's scihub username
    :param pword: ESA's scihub password
    :return:
    """

    # extract list of args
    if isinstance(filename, str):
        filename = Path(filename)

    with requests.Session() as session:

        session.auth = (uname, pword)
        request = session.request("get", url)
        response = session.get(request.url, auth=(uname, pword), stream=True)

        # raise an exception in case of http errors
        response.raise_for_status()

        logger.info(f"Downloading scene to: {filename}")
        # get download size
        total_length = int(response.headers.get("content-length", 0))

        # define chunk_size
        chunk_size = 1024

        # check if file is partially downloaded
        if filename.exists():
            first_byte = filename.stat().st_size
        else:
            first_byte = 0

        while first_byte < total_length:

            # get byte offset for already downloaded file
            header = {"Range": f"bytes={first_byte}-{total_length}"}
            response = session.get(url, headers=header, stream=True)

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
                        desc=" INFO: Downloading ",
                    )

                    for chunk in response.iter_content(chunk_size):
                        if chunk:
                            file.write(chunk)
                            pbar.update(chunk_size)

            pbar.close()
            # update file size
            first_byte = filename.stat().st_size

    zip_test = h.check_zipfile(filename)

    # if it did not pass the test, remove the file
    # in the while loop it will be downloaded again
    if zip_test is not None:
        if filename.exists():
            filename.unlink()
        raise DownloadError(
            f"{filename.name} did not pass the zip test. "
            f"Re-downloading the full scene."
        )
    else:
        logger.info(f"{filename.name} passed the zip test.")
        with open(filename.with_suffix(".downloaded"), "w+") as file:
            file.write("successfully downloaded \n")


def batch_download(inventory_df, download_dir, uname, pword, concurrent=10):

    from ost import Sentinel1Scene as S1Scene

    # create list with scene ids to download
    scenes = inventory_df["identifier"].tolist()

    asf_list = []
    for scene_id in scenes:

        # initialize scene instance and get destination filepath
        scene = S1Scene(scene_id)
        file_path = scene.download_path(download_dir, True)

        # check if already downloaded
        if file_path.with_suffix(".downloaded").exists():
            logger.info(f"{scene.scene_id} has been already downloaded.")
            continue

        # append to list
        asf_list.append([scene.asf_url(), file_path, uname, pword])

    # if list is not empty, do parallel download
    check_counter = 0
    if asf_list:
        executor = Executor(max_workers=concurrent, executor="concurrent_processes")

        for task in executor.as_completed(
            func=asf_download_parallel, iterable=asf_list, fargs=[]
        ):
            task.result()
            check_counter += 1

    # if all have been downloaded then we are through
    if len(inventory_df["identifier"].tolist()) == check_counter:
        logger.info("All products are downloaded.")
    # else we
    else:
        for scene in scenes:
            # we check if outputfile exists...
            scene = S1Scene(scene)
            file_path = scene.download_path(download_dir)
            if file_path.with_suffix(".downloaded").exists():
                # ...and remove from list of scenes to download
                scenes.remove(scene.scene_id)
            else:
                raise DownloadError(
                    "ASF download is incomplete or has failed. Try to re-run."
                )
