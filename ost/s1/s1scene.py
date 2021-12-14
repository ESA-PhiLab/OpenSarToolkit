#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""Class for handling a single Sentinel-1 product

This class, initialized by a valid Sentinel-1 scene identifier,
extracts basic metadata information from the scene ID itself,
as well as more detailed  and allows for retrieving OST relevant
paths.

For GRD products it is possible to pre-process the respective scene
based on a ARD product type.
"""

# ------ bug of rasterio --------
import os

if "GDAL_DATA" in list(os.environ.keys()):
    del os.environ["GDAL_DATA"]
if "PROJ_LIB" in list(os.environ.keys()):
    del os.environ["PROJ_LIB"]
# ------ bug of rasterio --------

import sys
import json
import logging
import zipfile
import fnmatch
import xml.dom.minidom
import xml.etree.ElementTree as eTree
import urllib.request
import urllib.parse
from urllib.error import URLError
from pathlib import Path

import requests
import pandas as pd
import geopandas as gpd

from ost.helpers import scihub, peps, onda, asf, raster as ras, helpers as h
from ost.helpers.settings import APIHUB_BASEURL, OST_ROOT
from ost.helpers.settings import set_log_level, check_ard_parameters
from ost.s1.grd_to_ard import grd_to_ard, ard_to_rgb

logger = logging.getLogger(__name__)

CONNECTION_ERROR = "We failed to connect to the server. Reason: "
CONNECTION_ERROR_2 = "The server couldn't fulfill the request. Error code: "


class Sentinel1Scene:
    def __init__(self, scene_id, ard_type="OST_GTC", log_level=logging.INFO):

        # set log level
        set_log_level(log_level)

        # get metadata from scene identifier
        self.scene_id = scene_id
        self.mission_id = scene_id[0:3]
        self.mode_beam = scene_id[4:6]
        self.product_type = scene_id[7:10]
        self.resolution_class = scene_id[10]
        self.proc_level = scene_id[12]
        self.pol_mode = scene_id[14:16]
        self.start_date = scene_id[17:25]
        self.start_time = scene_id[26:32]
        self.stop_date = scene_id[33:41]
        self.stop_time = scene_id[42:48]
        self.abs_orbit = scene_id[49:55]
        self.data_take_id = scene_id[57:62]
        self.unique_id = scene_id[63:]
        self.year = scene_id[17:21]
        self.month = scene_id[21:23]
        self.day = scene_id[23:25]
        self.onda_class = scene_id[4:14]

        # Calculate the relative orbit out of absolute orbit
        # (from Peter Meadows (ESA) @
        # http://forum.step.esa.int/t/sentinel-1-relative-orbit-from-filename/7042)
        if self.mission_id == "S1A":
            self.orbit_offset = 73
            self.satellite = "Sentinel-1A"
        elif self.mission_id == "S1B":
            self.orbit_offset = 27
            self.satellite = "Sentinel-1B"

        self.rel_orbit = ((int(self.abs_orbit) - int(self.orbit_offset)) % 175) + 1

        # get acquisition mode
        if self.mode_beam == "IW":
            self.acq_mode = "Interferometric Wide Swath"
        elif self.mode_beam.startswith("S"):
            self.acq_mode = "Stripmap"
        elif self.mode_beam == "EW":
            self.acq_mode = "Extra-Wide swath"
        elif self.mode_beam == "WV":
            self.acq_mode = "Wave"

        # get product type
        if self.product_type == "GRD":
            self.p_type = "Ground Range Detected (GRD)"
        elif self.product_type == "SLC":
            self.p_type = "Single-Look Complex (SLC)"
        elif self.product_type == "OCN":
            self.p_type = "Ocean"
        elif self.product_type == "RAW":
            self.p_type = "Raw Data (RAW)"

        # set initial product paths to None
        self.product_dl_path = None
        self.ard_dimap = None
        self.ard_rgb = None
        self.rgb_thumbnail = None

        # set initial ARD parameters to ard_type
        self.get_ard_parameters(ard_type)
        self.config_dict = dict(processing=self.ard_parameters)
        self.config_file = None

    def info(self):

        # actual print function
        print(" -------------------------------------------------")
        print(" Scene Information:")
        print(" Scene Identifier:        " + str(self.scene_id))
        print(" Satellite:               " + str(self.satellite))
        print(" Acquisition Mode:        " + str(self.acq_mode))
        print(" Processing Level:        " + str(self.proc_level))
        print(" Product Type:            " + str(self.p_type))
        print(" Acquisition Date:        " + str(self.start_date))
        print(" Start Time:              " + str(self.start_time))
        print(" Stop Time:               " + str(self.stop_time))
        print(" Absolute Orbit:          " + str(self.abs_orbit))
        print(" Relative Orbit:          " + str(self.rel_orbit))
        print(" -------------------------------------------------")

    def info_dict(self):

        # create info dictionary necessary for tests
        inf_dict = {}
        inf_dict.update(
            Scene_Identifier=str(self.scene_id),
            Satellite=str(self.satellite),
            Acquisition_Mode=str(self.acq_mode),
            Processing_Level=str(self.proc_level),
            Product_Type=str(self.p_type),
            Acquisition_Date=str(self.start_date),
            Start_Time=str(self.start_time),
            Stop_Time=str(self.stop_time),
            Absolute_Orbit=str(self.abs_orbit),
            Relative_Orbit=str(self.rel_orbit),
        )

        return inf_dict

    def download(self, download_dir, mirror=None):

        if not mirror:
            logger.info("One or more of your scenes need to be downloaded.")
            print(" Select the server from where you want to download:")
            print(" (1) Copernicus Apihub (ESA, rolling archive)")
            print(" (2) Alaska Satellite Facility (NASA, full archive)")
            print(" (3) PEPS (CNES, 1 year rolling archive)")
            print(
                " (4) ONDA DIAS (ONDA DIAS full archive for"
                " SLC - or GRD from 30 June 2019)"
            )
            # print(' (5) Alaska Satellite Facility (using WGET'
            #      ' - unstable - use only if 2 fails)')
            mirror = input(" Type 1, 2, 3, or 4: ")

        from ost.s1 import download

        if isinstance(download_dir, str):
            download_dir = Path(download_dir)

        if mirror == "1":
            uname, pword = scihub.ask_credentials()
            opener = scihub.connect(uname=uname, pword=pword)
            df = pd.DataFrame(
                {"identifier": [self.scene_id], "uuid": [self.scihub_uuid(opener)]}
            )

        elif mirror == "2":
            uname, pword = asf.ask_credentials()
            df = pd.DataFrame({"identifier": [self.scene_id]})

        elif mirror == "3":
            uname, pword = peps.ask_credentials()
            df = pd.DataFrame(
                {
                    "identifier": [self.scene_id],
                    "uuid": [self.peps_uuid(uname=uname, pword=pword)],
                }
            )
        elif mirror == "4":
            uname, pword = onda.ask_credentials()
            opener = onda.connect(uname=uname, pword=pword)
            df = pd.DataFrame(
                {"identifier": [self.scene_id], "uuid": [self.ondadias_uuid(opener)]}
            )
        else:
            raise ValueError("You entered the wrong mirror.")
        # else:  # ASF
        #    df = pd.DataFrame({'identifier': [self.scene_id]})
        #    download.download_sentinel1(df, download_dir, mirror)
        #    return

        download.download_sentinel1(df, download_dir, mirror, uname=uname, pword=pword)

        # delete credentials
        del uname, pword

    # location of file (including diases)
    def download_path(self, download_dir, mkdir=False):

        if isinstance(download_dir, str):
            download_dir = Path(download_dir)

        download_path = (
            Path(download_dir)
            / "SAR"
            / f"{self.product_type}"
            / f"{self.year}"
            / f"{self.month}"
            / f"{self.day}"
        )

        # make dir if not existent
        if mkdir:
            download_path.mkdir(parents=True, exist_ok=True)

        # get file_path
        file_path = download_path / f"{self.scene_id}.zip"

        self.product_dl_path = file_path

        return file_path

    def _creodias_path(self, data_mount):

        if isinstance(data_mount, str):
            data_mount = Path(data_mount)

        path = (
            Path(data_mount)
            / "Sentinel-1"
            / "SAR"
            / f"{self.product_type}"
            / f"{self.year}"
            / f"{self.month}"
            / f"{self.day}"
            / f"{self.scene_id}.SAFE"
        )

        return path

    def _onda_path(self, data_mount):

        path = (
            Path(data_mount)
            / "S1"
            / "LEVEL-1"
            / f"{self.onda_class}"
            / f"{self.year}"
            / f"{self.month}"
            / f"{self.day}"
            / f"{self.scene_id}.zip"
            / f"{self.scene_id}.SAFE"
        )

        return path

    def get_path(self, download_dir=None, data_mount=None):

        path = None
        if download_dir:

            # convert download_dir to Path object
            if isinstance(download_dir, str):
                download_dir = Path(download_dir)

            # construct download path
            self.download_path(download_dir=download_dir, mkdir=False)

            # check if scene is succesfully downloaded
            if self.product_dl_path.with_suffix(".downloaded").exists():
                path = self.product_dl_path

        if data_mount and not path:

            # covert data_mount to Path object
            if isinstance(data_mount, str):
                data_mount = Path(data_mount)

            # check creodias folder structure
            if (self._creodias_path(data_mount) / "manifest.safe").exists():
                path = self._creodias_path(data_mount)
            # check for ondadias folder structure
            elif self._onda_path(data_mount).exists():
                path = self._onda_path(data_mount)

        return path

    # scihub related
    def scihub_uuid(self, opener):

        # construct the basic the url
        base_url = "https://apihub.copernicus.eu/apihub/odata/v1/Products?$filter="

        # request
        action = urllib.parse.quote(f"Name eq '{self.scene_id}'")

        # construct the download url
        url = base_url + action

        try:
            # get the request
            req = opener.open(url)
        except URLError as error:
            if hasattr(error, "reason"):
                logger.info(f"{CONNECTION_ERROR}{error.reason}")
                sys.exit()
            elif hasattr(error, "code"):
                logger.info(f"{CONNECTION_ERROR_2}{error.reason}")
                sys.exit()
        else:
            # write the request to to the response variable
            # (i.e. the xml coming back from scihub)
            response = req.read().decode("utf-8")

            # return uuid from response
            return response.split("Products('")[1].split("')")[0]

    def scihub_url(self, opener):

        # return the full url
        return f"{APIHUB_BASEURL}('{self.scihub_uuid(opener)}')/$value"

    def scihub_md5(self, opener):

        # return the md5 checksum
        return (
            f"{APIHUB_BASEURL}('{self.scihub_uuid(opener)}')" f"/Checksum/Value/$value"
        )

    def scihub_online_status(self, opener):

        # get url for product
        url = f"{APIHUB_BASEURL}('{self.scihub_uuid(opener)}')/Online/$value"

        # check if something is coming back from our request
        try:
            # get the request
            req = opener.open(url)
        except URLError as error:
            if hasattr(error, "reason"):
                logger.info(f"{CONNECTION_ERROR}{error.reason}")
                sys.exit()
            elif hasattr(error, "code"):
                logger.info(f"{CONNECTION_ERROR_2}{error.reason}")
                sys.exit()
        else:
            # write the request to to the response variable
            # (i.e. the xml coming back from scihub)
            response = req.read().decode("utf-8")

            if response == "true":
                response = True
            elif response == "false":
                response = False
            else:
                raise TypeError("Wrong response type.")

            return response

    def scihub_trigger_production(self, opener):

        # get uuid and construct url for scihub's apihub
        uuid = self.scihub_uuid(opener)
        url = f"{APIHUB_BASEURL}('{uuid}')/$value"

        try:
            # get the request
            req = opener.open(url)
        except URLError as error:
            if hasattr(error, "reason"):
                logger.info(f"{CONNECTION_ERROR}{error.reason}")
                sys.exit()
            elif hasattr(error, "code"):
                logger.info(f"{CONNECTION_ERROR_2}{error.reason}")
                sys.exit()
        else:
            # write the request to to the response variable
            # (i.e. the xml coming back from scihub)
            code = req.getcode()
            if code == 202:
                logging.info(f"Production of {self.scene_id} successfully requested.")

            return code

    def _scihub_annotation_url(self, opener):
        """Retrieve the urls for the product annotation files

        :param opener:
        :type opener:
        :return: the urls for the product annotation files
        :rtype: list
        """

        # get uuid for product
        uuid = self.scihub_uuid(opener)

        logger.info(
            f"Retrieving URLs of annotation files for S1 product: " f"{self.scene_id}."
        )
        scihub_url = "https://apihub.copernicus.eu/apihub/odata/v1/Products"
        anno_path = (
            f"('{uuid}')/Nodes('{self.scene_id}.SAFE')" f"/Nodes('annotation')/Nodes"
        )

        # construct anno url path
        url = scihub_url + anno_path

        # try to retrieve
        try:
            # get the request
            req = opener.open(url)
        except URLError as error:
            if hasattr(error, "reason"):
                logger.info(f"{CONNECTION_ERROR}{error.reason}")
                sys.exit()
            elif hasattr(error, "code"):
                logger.info(f"{CONNECTION_ERROR_2}{error.reason}")
                sys.exit()
        else:
            # read out the response
            response = req.read().decode("utf-8")

            # parse the response
            dom = xml.dom.minidom.parseString(response)

            # loop through each entry (with all metadata)
            url_list = []
            for node in dom.getElementsByTagName("entry"):
                download_url = node.getElementsByTagName("id")[0].firstChild.nodeValue

                # if we find an xml we append to the list of relevant files
                if download_url[-6:-2] == ".xml":
                    url_list.append(f"{download_url}/$value")

            return url_list

    def scihub_annotation_get(self, uname=None, pword=None):
        """

        :param uname:
        :param pword:
        :return:
        """

        from ost.s1.burst_inventory import burst_extract

        # create emtpy geodataframe to be filled with burst infos
        column_names = [
            "SceneID",
            "Track",
            "Date",
            "SwathID",
            "AnxTime",
            "BurstNr",
            "geometry",
        ]
        gdf_final = gpd.GeoDataFrame(columns=column_names)

        # get connected to scihub
        opener = scihub.connect(uname, pword)
        anno_list = self._scihub_annotation_url(opener)

        for url in anno_list:
            try:
                # get the request
                req = opener.open(url)
            except URLError as error:
                if hasattr(error, "reason"):
                    logger.info(f"{CONNECTION_ERROR}{error.reason}")
                    sys.exit()
                elif hasattr(error, "code"):
                    logger.info(f"{CONNECTION_ERROR_2}{error.reason}")
                    sys.exit()
            else:
                # write the request to to the response variable
                # (i.e. the xml coming back from scihub)
                response = req.read().decode("utf-8")

                et_root = eTree.fromstring(response)

                # parse the xml page from the response
                gdf = burst_extract(
                    self.scene_id, self.rel_orbit, self.start_date, eTree.parse(et_root)
                )

                gdf_final = gdf_final.append(gdf)

        return gdf_final.drop_duplicates(["AnxTime"], keep="first")

    def zip_annotation_get(self, download_dir, data_mount=None):
        """

        :param download_dir:
        :param data_mount:
        :return:
        """

        from ost.s1.burst_inventory import burst_extract

        column_names = [
            "SceneID",
            "Track",
            "Date",
            "SwathID",
            "AnxTime",
            "BurstNr",
            "geometry",
        ]

        # crs for empty dataframe
        crs = "epsg:4326"
        gdf_final = gpd.GeoDataFrame(columns=column_names, crs=crs)

        file = self.get_path(download_dir, data_mount)

        # extract info from archive
        archive = zipfile.ZipFile(file, "r")
        namelist = archive.namelist()
        anno_files = fnmatch.filter(namelist, "*/annotation/s*.xml")

        # loop through xml annotation files
        for anno_file in anno_files:
            anno_string = archive.open(anno_file)

            gdf = burst_extract(
                self.scene_id, self.rel_orbit, self.start_date, eTree.parse(anno_string)
            )

            gdf_final = gdf_final.append(gdf)

        return gdf_final.drop_duplicates(["AnxTime"], keep="first")

    def safe_annotation_get(self, download_dir, data_mount=None):

        from ost.s1.burst_inventory import burst_extract

        column_names = [
            "SceneID",
            "Track",
            "Date",
            "SwathID",
            "AnxTime",
            "BurstNr",
            "geometry",
        ]
        gdf_final = gpd.GeoDataFrame(columns=column_names)

        file_path = self.get_path(download_dir=download_dir, data_mount=data_mount)

        for anno_file in list(file_path.glob("annotation/*xml")):
            # parse the xml page from the response
            gdf = burst_extract(
                self.scene_id, self.rel_orbit, self.start_date, eTree.parse(anno_file)
            )
            gdf_final = gdf_final.append(gdf)

        return gdf_final.drop_duplicates(["AnxTime"], keep="first")

    # onda dias uuid extractor
    def ondadias_uuid(self, opener):

        # construct the basic the url
        base_url = "https://catalogue.onda-dias.eu/dias-catalogue/Products?$search="

        # construct the download url
        action = '"' + self.scene_id + '.zip"'
        url = base_url + action

        try:
            # get the request
            req = opener.open(url)
        except URLError as error:
            if hasattr(error, "reason"):
                logger.info(f"{CONNECTION_ERROR}{error.reason}")
                sys.exit()
            elif hasattr(error, "code"):
                logger.info(f"{CONNECTION_ERROR_2}{error.reason}")
                sys.exit()
        else:
            # write the request to to the response variable
            # (i.e. the xml coming back from onda dias)
            response = req.read().decode("utf-8")

            # parse the uuid from the response (a messy pseudo xml)
            uuid = response.split('":"')[3].split('","')[0]
            return uuid

    # other data providers
    def asf_url(self):
        """Constructor for ASF download URL

        :return: string of the ASF download url
        :rtype: str
        """
        # base url of ASF
        asf_url = "https://datapool.asf.alaska.edu"

        # get ASF style mission id
        if self.mission_id == "S1A":
            mission = "SA"
        elif self.mission_id == "S1B":
            mission = "SB"
        else:
            raise ValueError("Wrong mission id.")

        # get relevant product type in ASF style
        if self.product_type == "SLC":
            product_type = self.product_type
        elif self.product_type == "GRD":
            product_type = f"GRD_{self.resolution_class}{self.pol_mode[0]}"
        else:
            raise ValueError("Wrong product type.")

        return f"{asf_url}/{product_type}/{mission}/{self.scene_id}.zip"

    def peps_uuid(self, uname, pword):
        """Retrieval of the PEPS UUID from the Peps server

        :param uname: username for CNES' PEPS server
        :param pword: password for CNES' PEPS server
        :return:
        """
        # construct product url
        url = (
            f"https://peps.cnes.fr/resto/api/collections/S1/search.json?q="
            f"{self.scene_id}"
        )

        # get response
        response = requests.get(url, stream=True, auth=(uname, pword))

        # check response
        if response.status_code == 401:
            raise ValueError(" ERROR: Username/Password are incorrect.")
        elif response.status_code != 200:
            response.raise_for_status()

        data = json.loads(response.text)
        peps_uuid = data["features"][0]["id"]
        download_url = data["features"][0]["properties"]["services"]["download"]["url"]

        return peps_uuid, download_url

    def peps_online_status(self, uname, pword):
        """Check if product is online at CNES' Peps server.

        :param uname: CNES' Peps username
        :param pword: CNES' Peps password
        :return:
        """

        _, url = self.peps_uuid(uname, pword)

        # define url
        response = requests.get(url, stream=True, auth=(uname, pword))
        status = response.status_code

        # check response
        if status == 401:
            raise ValueError(" ERROR: Username/Password are incorrect.")
        elif status == 404:
            raise ValueError(" ERROR: File not found.")
        elif status == 200:
            status = "online"
        elif status == 202:
            status = "onTape"
        else:
            response.raise_for_status()

        return status, url

    def get_ard_parameters(self, ard_type):

        # find respective template for selected ARD type
        template_file = (
            OST_ROOT
            / "graphs"
            / "ard_json"
            / f"{self.product_type.lower()}.{ard_type.replace('-', '_').lower()}.json"
        )

        # open and load parameters
        with open(template_file, "r") as ard_file:
            self.ard_parameters = json.load(ard_file)["processing"]

    def update_ard_parameters(self, ard_type=None):
        """

        :param ard_type:
        :return:
        """
        # if a ard type is selected, load
        if ard_type:
            self.get_ard_parameters(ard_type)

        # check for correctness of ard parameters
        check_ard_parameters(self.ard_parameters)

        # re-create project dict with update ard parameters
        self.config_dict.update(processing=self.ard_parameters)

        # dump to json file
        if self.config_file:
            with open(self.config_file, "w") as outfile:
                json.dump(self.config_dict, outfile, indent=4)

    def set_external_dem(self, dem_file, ellipsoid_correction=True):
        """

        :param dem_file:
        :param ellipsoid_correction:
        :return:
        """

        import rasterio

        # check if file exists
        if not Path(dem_file).exists():
            raise FileNotFoundError(f"No file found at {dem_file}.")

        # get no data value
        with rasterio.open(dem_file) as file:
            dem_nodata = file.nodata

        # get resampling
        img_res = self.ard_parameters["single_ARD"]["dem"]["image_resampling"]
        dem_res = self.ard_parameters["single_ARD"]["dem"]["dem_resampling"]

        # update ard parameters
        dem_dict = dict(
            {
                "dem_name": "External DEM",
                "dem_file": dem_file,
                "dem_nodata": dem_nodata,
                "dem_resampling": dem_res,
                "image_resampling": img_res,
                "egm_correction": ellipsoid_correction,
                "out_projection": "WGS84(DD)",
            }
        )

        # update ard_parameters
        self.ard_parameters["single_ARD"]["dem"] = dem_dict

    def create_ard(self, infile, out_dir, subset=None, overwrite=False):
        """

        :param infile:
        :param out_dir:
        :param subset:
        :param overwrite:
        :return:
        """

        if self.product_type != "GRD":
            raise ValueError(
                "The create_ard method for single products is currently "
                "only available for GRD products"
            )

        if isinstance(infile, str):
            infile = Path(infile)

        if isinstance(out_dir, str):
            out_dir = Path(out_dir)

        # set config param necessary for processing
        self.config_dict["processing_dir"] = str(out_dir)
        self.config_dict["temp_dir"] = str(out_dir / "temp")
        self.config_dict["snap_cpu_parallelism"] = os.cpu_count()
        self.config_dict["subset"] = False

        if subset:
            self.config_dict["subset"] = True
            self.config_dict["aoi"] = subset

        # create directories
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "temp").mkdir(parents=True, exist_ok=True)

        if overwrite:
            file_dir = out_dir / f"{self.rel_orbit}" / f"{self.start_date}"
            if (file_dir / ".processed").exists():
                (file_dir / ".processed").unlink()

        # --------------------------------------------
        # 2 Check if within SRTM coverage
        # set ellipsoid correction and force GTC production
        # when outside SRTM
        center_lat = self._get_center_lat(infile)
        if float(center_lat) > 59 or float(center_lat) < -59:

            if "SRTM" in self.ard_parameters["single_ARD"]["dem"]["dem_name"]:

                logger.info(
                    "Scene is outside SRTM coverage. Snap will therefore use "
                    "the Copernicus 30m Global DEM. "
                )

                self.ard_parameters["single_ARD"]["dem"][
                    "dem_name"
                ] = "Copernicus 30m Global DEM"

            if self.ard_parameters["single_ARD"]["dem"]["out_projection"] == 4326:

                logger.info(
                    "The scene's location is towards the poles. "
                    "Consider to use a stereographic projection."
                )

                epsg = input(
                    "Type an alternative EPSG code for the projection of the "
                    "output data or just press enter for keeping Lat/Lon "
                    "coordinate system (e.g. 3413 for NSIDC Sea Ice Polar "
                    "Stereographic North projection, or 3976 for "
                    "NSIDC Sea Ice Polar Stereographic South projection"
                )

                if not epsg:
                    epsg = 4326

                self.ard_parameters["single_ARD"]["dem"]["out_projection"] = int(epsg)

        # --------------------------------------------
        # 3 Check ard parameters in case they have been updated,
        #   and write them to json file

        # set config file to output directory
        self.config_file = out_dir / "processing.json"

        # write ard parameters, and check if they are correct
        self.update_ard_parameters()

        # --------------------------------------------
        # 4 set resolution to degree
        # self.ard_parameters['resolution'] = h.resolution_in_degree(
        #    self.center_lat, self.ard_parameters['resolution'])

        # --------------------------------------------
        # 5 run the burst to ard batch routine
        filelist, out_bs, out_ls, error = grd_to_ard(
            [infile],
            self.config_file,
        )

        # print error if any
        if error:
            logger.info(error)
        else:
            # remove temp folder
            tmp_folder = out_dir / "temp"
            h.remove_folder_content(tmp_folder)
            tmp_folder.rmdir()
            self.ard_dimap = out_bs

    def create_rgb(self, outfile, driver="GTiff"):

        # invert ot db from create_ard workflow for rgb creation
        # (otherwise we do it double)
        if self.ard_parameters["single_ARD"]["to_db"]:
            to_db = False
        else:
            to_db = True

        ard_to_rgb(self.ard_dimap, outfile, driver, to_db)
        self.ard_rgb = outfile

    def create_rgb_thumbnail(self, outfile, shrink_factor=25):

        # invert ot db from create_ard workflow for rgb creation
        # (otherwise we do it double)
        if self.ard_parameters["single_ARD"]["to_db"]:
            to_db = False
        else:
            to_db = True

        self.rgb_thumbnail = outfile
        driver = "JPEG"
        ard_to_rgb(self.ard_dimap, self.rgb_thumbnail, driver, to_db, shrink_factor)

    def visualise_rgb(self, shrink_factor=25):

        ras.visualise_rgb(self.ard_rgb, shrink_factor)

    # other functions
    def _get_center_lat(self, scene_path=None):

        if scene_path.suffix == ".zip":
            zip_archive = zipfile.ZipFile(str(scene_path))
            manifest = zip_archive.read(f"{self.scene_id}.SAFE/manifest.safe")
        elif scene_path.suffix == ".SAFE":
            with (scene_path / "manifest.safe").open("rb") as file:
                manifest = file.read()
        else:
            raise ValueError("Invalid file.")

        root = eTree.fromstring(manifest)
        coordinates = None
        for child in root:
            metadata = child.findall("metadataObject")
            for meta in metadata:
                for wrap in meta.findall("metadataWrap"):
                    for data in wrap.findall("xmlData"):
                        for frame_set in data.findall(
                            "{http://www.esa.int/safe/sentinel-1.0}" "frameSet"
                        ):
                            for frame in frame_set.findall(
                                "{http://www.esa.int/safe/sentinel-1.0}" "frame"
                            ):
                                for footprint in frame.findall(
                                    "{http://www.esa.int/" "safe/sentinel-1.0}footPrint"
                                ):
                                    for coords in footprint.findall(
                                        "{http://www.opengis.net/gml}" "coordinates"
                                    ):
                                        coordinates = coords.text.split(" ")

        if coordinates:
            sums, i = 0, 0
            for i, coords in enumerate(coordinates):
                sums = sums + float(coords.split(",")[0])

            return sums / (i + 1)
        else:
            raise RuntimeError(
                "Could not find any coordinates within the metadata file"
            )
