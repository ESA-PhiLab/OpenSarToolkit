#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""Module for batch download of Sentinel-1 based on OST inventory

This module handles the download of Sentinel-1, offering download capabilities
from different servers such as Copernicus Scihub, Alaska Satellite Facility's
vertex as well as PEPS from CNES.

"""

import getpass
import logging
from pathlib import Path

from ost.s1.s1scene import Sentinel1Scene as S1Scene
from ost.helpers import helpers as h
from ost.helpers import scihub, peps, asf, onda  # , asf_wget

logger = logging.getLogger(__name__)


def restore_download_dir(input_directory, download_dir):
    """Create the OST download directory structure from downloaded files

    In case data is already downloaded to a single folder, this function can
    be used to create a OST compliant structure of the download directory.

    :param input_directory: the directory, where the downloaded files
                            are located
    :type input_directory: str/Path
    :param download_dir: the high-level directory compliant with OST
    :type download_dir: str/Path
    """

    if isinstance(input_directory, str):
        input_directory = Path(input_directory)

    if isinstance(download_dir, str):
        download_dir = Path(download_dir)

    for scene in list(input_directory.glob("*zip")):

        # get scene
        s1scene = S1Scene(scene.name[:-4])

        # create download path and file
        file_path = s1scene.download_path(download_dir, True)

        # check zipfile
        logger.info(f"Checking zip file {str(scene)} for inconsistency.")
        zip_test = h.check_zipfile(str(scene))

        if not zip_test:

            logger.info("Passed zip file test.")
            scene.rename(file_path)

            # add downloaded (should be zip checked in future)
            with open(file_path.with_suffix(".downloaded"), "w+") as file:
                file.write("successfully downloaded \n")
        else:
            logger.info(f"File {str(scene)} is corrupted and will not be moved.")


def download_sentinel1(
    inventory_df, download_dir, mirror=None, concurrent=2, uname=None, pword=None
):
    """Function to download Sentinel-1 with choice of data repository

    :param inventory_df: OST inventory dataframe
    :type inventory_df: GeoDataFrame
    :param download_dir: high-level download directory
    :type download_dir: Path
    :param mirror: number of data repository \n
                1 - Scihub \n
                2 - ASF \n
                3 - PEPS \n
                4 - ONDA \n
    :type mirror: int
    :param concurrent: number of parallel downloads
    :type concurrent: int
    :param uname: username for respective data repository
    :type uname: str
    :param pword: password for respective data repository
    :type pword: str
    """

    if not mirror:
        print(" Select the server from where you want to download:")
        print(" (1) Copernicus Apihub (ESA, rolling archive)")
        print(" (2) Alaska Satellite Facility (NASA, full archive)")
        print(" (3) PEPS (CNES, 1 year rolling archive)")
        print(
            " (4) ONDA DIAS (ONDA DIAS full archive for SLC -"
            " or GRD from 30 June 2019)"
        )
        # print(' (5) Alaska Satellite Facility (using WGET - '
        # 'unstable - use only if 2 does not work)')
        mirror = input(" Type 1, 2, 3, or 4: ")

    if not uname:
        print(" Please provide username for the selected server")
        uname = input(" Username:")

    if not pword:
        print(" Please provide password for the selected server")
        pword = getpass.getpass(" Password:")

    # check if uname and pwoed are correct
    if int(mirror) == 1:
        error_code = scihub.check_connection(uname, pword)
    elif int(mirror) == 2:
        error_code = asf.check_connection(uname, pword)

        if concurrent > 10:
            logger.info(
                "Maximum allowed parallel downloads from Earthdata are 10. "
                "Setting concurrent accordingly."
            )
            concurrent = 10

    elif int(mirror) == 3:
        error_code = peps.check_connection(uname, pword)
    elif int(mirror) == 4:
        error_code = onda.check_connection(uname, pword)
    # elif int(mirror) == 5:
    #    error_code = asf_wget.check_connection(uname, pword)
    # hidden option for downloading from czech mirror
    elif int(mirror) == 321:
        error_code = scihub.check_connection(
            uname, pword, base_url="https://dhr1.cesnet.cz/"
        )
    else:
        raise ValueError("No valid mirror selected")

    if error_code == 401:
        raise ValueError("Username/Password are incorrect")
    elif error_code != 200:
        raise ValueError(f"Some connection error. Error code {error_code}.")

    # download in parallel
    if int(mirror) == 1:  # scihub
        scihub.batch_download(inventory_df, download_dir, uname, pword, concurrent)
    elif int(mirror) == 2:  # ASF
        asf.batch_download(inventory_df, download_dir, uname, pword, concurrent)
    elif int(mirror) == 3:  # PEPS
        peps.batch_download(inventory_df, download_dir, uname, pword, concurrent)
    elif int(mirror) == 4:  # ONDA DIAS
        onda.batch_download(inventory_df, download_dir, uname, pword, concurrent)
    if int(mirror) == 321:  # scihub czech mirror
        scihub.batch_download(
            inventory_df,
            download_dir,
            uname,
            pword,
            concurrent,
            base_url="https://dhr1.cesnet.cz/",
        )
    # elif int(mirror) == 5:    # ASF WGET
    #    asf_wget.batch_download(inventory_df, download_dir,
    #                            uname, pword, concurrent)
