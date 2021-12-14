#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ------ bug of rasterio --------
import os

if "GDAL_DATA" in list(os.environ.keys()):
    del os.environ["GDAL_DATA"]
if "PROJ_LIB" in list(os.environ.keys()):
    del os.environ["PROJ_LIB"]
# ------ bug of rasterio --------

import json
import urllib.request
import urllib.parse
import logging
from pathlib import Path
from datetime import datetime
from multiprocessing import cpu_count

import rasterio
import geopandas as gpd
from shapely.wkt import loads

from ost.helpers import vector as vec, raster as ras
from ost.helpers import scihub, helpers as h, srtm
from ost.helpers.settings import set_log_level, setup_logfile, OST_ROOT
from ost.helpers.settings import check_ard_parameters

from ost.s1 import search, refine_inventory, download
from ost.s1 import burst_inventory, burst_batch
from ost.s1 import grd_batch

# get the logger
logger = logging.getLogger(__name__)

# global variables
OST_DATEFORMAT = "%Y-%m-%d"
OST_INVENTORY_FILE = "full.inventory.gpkg"


class Generic:
    def __init__(
        self,
        project_dir,
        aoi,
        start="1978-06-28",
        end=datetime.today().strftime(OST_DATEFORMAT),
        data_mount=None,
        log_level=logging.INFO,
    ):

        # ------------------------------------------
        # 1 Start logger
        # set log level to logging.INFO as standard
        set_log_level(log_level)

        # ------------------------------------------
        # 2 Handle directories

        # get absolute path to project directory
        self.project_dir = Path(project_dir).resolve()

        # create project directory if not existent
        try:
            self.project_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created project directory at {self.project_dir}")
        except FileExistsError:
            logger.info(
                "Project directory already exists. "
                "No data has been deleted at this point but "
                "make sure you really want to use this folder."
            )

        # define project sub-directories if not set, and create folders
        self.download_dir = self.project_dir / "download"
        self.download_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Downloaded data will be stored in: {self.download_dir}.")

        self.inventory_dir = self.project_dir / "inventory"
        self.inventory_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Inventory files will be stored in: {self.inventory_dir}.")

        self.processing_dir = self.project_dir / "processing"
        self.processing_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Processed data will be stored in: {self.processing_dir}.")

        self.temp_dir = self.project_dir / "temp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Using {self.temp_dir} as directory for temporary files.")

        # ------------------------------------------
        # 3 create a standard logfile
        setup_logfile(self.project_dir / ".processing.log")

        # ------------------------------------------
        # 4 handle AOI (read and get back WKT)
        self.aoi = vec.aoi_to_wkt(aoi)

        # ------------------------------------------
        # 5 Handle Period of Interest
        try:
            datetime.strptime(start, OST_DATEFORMAT)
            self.start = start
        except ValueError:
            raise ValueError(
                "Incorrect date format for start date. " "It should be YYYY-MM-DD"
            )

        try:
            datetime.strptime(end, OST_DATEFORMAT)
            self.end = end
        except ValueError:
            raise ValueError(
                "Incorrect date format for end date. " "It should be YYYY-MM-DD"
            )

        # ------------------------------------------
        # 6 Check data mount
        if data_mount:
            if Path(data_mount).exists():
                self.data_mount = Path(data_mount)
            else:
                raise NotADirectoryError(f"{data_mount} is not a directory.")
        else:
            self.data_mount = None

        # ------------------------------------------
        # 7 put all parameters in a dictionary
        self.config_dict = {
            "project_dir": str(self.project_dir),
            "download_dir": str(self.download_dir),
            "inventory_dir": str(self.inventory_dir),
            "processing_dir": str(self.processing_dir),
            "temp_dir": str(self.temp_dir),
            "data_mount": str(self.data_mount),
            "aoi": self.aoi,
            "start_date": self.start,
            "end_date": self.end,
        }


class Sentinel1(Generic):
    """A Sentinel-1 specific subclass of the Generic OST class
    This subclass creates a Sentinel-1 specific
    """

    def __init__(
        self,
        project_dir,
        aoi,
        start="2014-10-01",
        end=datetime.today().strftime(OST_DATEFORMAT),
        data_mount=None,
        product_type="*",
        beam_mode="*",
        polarisation="*",
        log_level=logging.INFO,
    ):

        # ------------------------------------------
        # 1 Get Generic class attributes
        super().__init__(project_dir, aoi, start, end, data_mount, log_level)

        # ------------------------------------------
        # 2 Check and set product type
        if product_type in ["*", "RAW", "SLC", "GRD"]:
            self.product_type = product_type
        else:
            raise ValueError("Product type must be one out of '*', 'RAW', 'SLC', 'GRD'")

        # ------------------------------------------
        # 3 Check and set beam mode
        if beam_mode in ["*", "IW", "EW", "SM"]:
            self.beam_mode = beam_mode
        else:
            raise ValueError("Beam mode must be one out of 'IW', 'EW', 'SM'")

        # ------------------------------------------
        # 4 Check and set polarisations
        possible_pols = ["*", "VV", "VH", "HV", "HH", "VV VH", "HH HV"]
        if polarisation in possible_pols:
            self.polarisation = polarisation
        else:
            raise ValueError(f"Polarisation must be one out of {possible_pols}")

        # ------------------------------------------
        # 5 Initialize the inventory file
        inventory_file = self.inventory_dir / OST_INVENTORY_FILE
        if inventory_file.exists():
            self.inventory_file = inventory_file
            logging.info(
                "Found an existing inventory file. This can be overwritten "
                "by re-executing the search."
            )
            self.read_inventory()
        else:
            self.inventory = None
            self.inventory_file = None

        # ------------------------------------------
        # 6 Initialize refinements
        self.refined_inventory_dict = None
        self.coverages = None

        # ------------------------------------------
        # 7 Initialize burst inventories
        self.burst_inventory = None
        self.burst_inventory_file = None

        # ------------------------------------------
        # 7 Initialize uname and pword to None
        self.scihub_uname = None
        self.scihub_pword = None

        self.asf_uname = None
        self.asf_pword = None

        self.peps_uname = None
        self.peps_pword = None

        self.onda_uname = None
        self.onda_pword = None

    # ------------------------------------------
    # methods
    def search(
        self,
        outfile=OST_INVENTORY_FILE,
        append=False,
        base_url="https://apihub.copernicus.eu/apihub",
    ):
        """High Level search function

        :param outfile:
        :param append:
        :param base_url:
        :return:
        """

        # create scihub conform aoi string
        aoi = scihub.create_aoi_str(self.aoi)

        # create scihub conform TOI
        toi = scihub.create_toi_str(self.start, self.end)

        # create scihub conform product specification
        product_specs = scihub.create_s1_product_specs(
            self.product_type, self.polarisation, self.beam_mode
        )

        # construct the final query
        query = urllib.parse.quote(
            f"Sentinel-1 AND {product_specs} AND {aoi} AND {toi}"
        )

        if not self.scihub_uname or not self.scihub_pword:
            # ask for username and password
            self.scihub_uname, self.scihub_pword = scihub.ask_credentials()

        # do the search
        if outfile == OST_INVENTORY_FILE:
            self.inventory_file = self.inventory_dir / OST_INVENTORY_FILE
        else:
            self.inventory_file = outfile

        search.scihub_catalogue(
            query,
            self.inventory_file,
            append,
            self.scihub_uname,
            self.scihub_pword,
            base_url,
        )

        if self.inventory_file.exists():
            # read inventory into the inventory attribute
            self.read_inventory()
        else:
            logger.info("No images found in the AOI for this date range")

    def read_inventory(self):
        """Read the Sentinel-1 data inventory from a OST invetory shapefile
        :param
        """

        # define column names of inventory file (since in shp they are
        # truncated)
        column_names = [
            "id",
            "identifier",
            "polarisationmode",
            "orbitdirection",
            "acquisitiondate",
            "relativeorbit",
            "orbitnumber",
            "product_type",
            "slicenumber",
            "size",
            "beginposition",
            "endposition",
            "lastrelativeorbitnumber",
            "lastorbitnumber",
            "uuid",
            "platformidentifier",
            "missiondatatakeid",
            "swathidentifier",
            "ingestiondate",
            "sensoroperationalmode",
            "geometry",
        ]

        geodataframe = gpd.read_file(self.inventory_file)
        geodataframe.columns = column_names

        # add download_path to inventory, so we can check if data needs to be
        # downloaded
        self.inventory = search.check_availability(
            geodataframe, self.download_dir, self.data_mount
        )

    def download_size(self, inventory_df=None):
        """Function to get the total size of all products when extracted in GB

        :param inventory_df:
        :return:
        """
        if inventory_df is None:
            download_size = (
                self.inventory["size"].str.replace(" GB", "").astype("float32").sum()
            )
        else:
            download_size = (
                inventory_df["size"].str.replace(" GB", "").astype("float32").sum()
            )

        logger.info(f"There are about {download_size} GB need to be downloaded.")

    def refine_inventory(
        self,
        exclude_marginal=True,
        full_aoi_crossing=True,
        mosaic_refine=True,
        area_reduce=0.05,
        complete_coverage=True,
    ):

        (
            self.refined_inventory_dict,
            self.coverages,
        ) = refine_inventory.search_refinement(
            self.aoi,
            self.inventory,
            self.inventory_dir,
            exclude_marginal=exclude_marginal,
            full_aoi_crossing=full_aoi_crossing,
            mosaic_refine=mosaic_refine,
            area_reduce=area_reduce,
            complete_coverage=complete_coverage,
        )

        # summing up information
        print("--------------------------------------------")
        print(" Summing up the info about mosaics")
        print("--------------------------------------------")
        for key in self.refined_inventory_dict:
            print("")
            print(f" {self.coverages[key]} mosaics for mosaic key {key}")

    def download(self, inventory_df, mirror=None, concurrent=2, uname=None, pword=None):

        # if an old inventory exists drop download_path
        if "download_path" in inventory_df:
            inventory_df.drop("download_path", axis=1)

        # check if scenes exist
        inventory_df = search.check_availability(
            inventory_df, self.download_dir, self.data_mount
        )

        # extract only those scenes that need to be downloaded
        download_df = inventory_df[inventory_df.download_path == "None"]

        # to download or not ot download - that is here the question
        if download_df.empty:
            logger.info("All scenes are ready for being processed.")
        else:
            logger.info("One or more scene(s) need(s) to be downloaded.")
            download.download_sentinel1(
                download_df,
                self.download_dir,
                mirror=mirror,
                concurrent=concurrent,
                uname=uname,
                pword=pword,
            )

    def create_burst_inventory(
        self, inventory_df=None, refine=True, outfile=None, uname=None, pword=None
    ):

        # assert SLC product type
        if not self.product_type == "SLC":
            raise ValueError(
                "Burst inventory is only possible for the SLC product type"
            )

        # in case a custom inventory is given (e.g. a refined inventory)
        if inventory_df is None:
            inventory_df = self.inventory
        else:
            # assert that all products are SLCs
            if not inventory_df.product_type.unique() == "SLC":
                raise ValueError(
                    "The inventory dataframe can only contain SLC products "
                    "for the burst inventory "
                )

        if not outfile:
            outfile = self.inventory_dir / "burst_inventory.gpkg"

        # run the burst inventory
        self.burst_inventory = burst_inventory.burst_inventory(
            inventory_df,
            outfile,
            download_dir=self.download_dir,
            data_mount=self.data_mount,
            uname=uname,
            pword=pword,
        )

        # refine the burst inventory
        if refine:
            self.burst_inventory = burst_inventory.refine_burst_inventory(
                self.aoi, self.burst_inventory, f"{str(outfile)[:-5]}.refined.gpkg"
            )

    def read_burst_inventory(self, burst_file=None):
        """
        :param burst_file: a GeoPackage file created by OST holding a burst
                           inventory
        :return: geodataframe
        """
        if not burst_file:

            non_ref = self.inventory_dir / "burst_inventory.gpkg"
            refined = self.inventory_dir / "burst_inventory.refined.gpkg"

            if refined.exists():
                logger.info(f"Importing refined burst inventory file {str(refined)}.")
                burst_file = refined
            elif non_ref.exists():
                logger.info(f"Importing refined burst inventory file {str(non_ref)}.")
                burst_file = non_ref
            else:
                raise FileNotFoundError(
                    "No previously created burst inventory file " "has been found."
                )
        # define column names of file (since in shp they are truncated)
        # create column names for empty data frame
        column_names = [
            "SceneID",
            "Track",
            "Direction",
            "Date",
            "SwathID",
            "AnxTime",
            "BurstNr",
            "geometry",
        ]

        geodataframe = gpd.read_file(burst_file)
        geodataframe = geodataframe[column_names]
        self.burst_inventory = geodataframe

        return geodataframe

    def plot_inventory(self, inventory_df=None, transparency=0.05, annotate=False):

        if inventory_df is None:
            vec.plot_inventory(self.aoi, self.inventory, transparency, annotate)
        else:
            vec.plot_inventory(self.aoi, inventory_df, transparency, annotate)


class Sentinel1Batch(Sentinel1):
    """A Sentinel-1 specific subclass of the Generic OST class
    This subclass creates a Sentinel-1 specific
    """

    def __init__(
        self,
        project_dir,
        aoi,
        start="2014-10-01",
        end=datetime.today().strftime(OST_DATEFORMAT),
        data_mount=None,
        product_type="SLC",
        beam_mode="IW",
        polarisation="*",
        ard_type="OST-GTC",
        snap_cpu_parallelism=cpu_count(),
        max_workers=1,
        log_level=logging.INFO,
    ):
        # ------------------------------------------
        # 1 Initialize super class
        super().__init__(
            project_dir,
            aoi,
            start,
            end,
            data_mount,
            product_type,
            beam_mode,
            polarisation,
            log_level,
        )

        # ---------------------------------------
        # 1 Check and set ARD type

        # possible ard types to select from for GRD
        if product_type == "GRD":
            ard_types_grd = ["CEOS", "Earth-Engine", "OST-GTC", "OST-RTC"]
            if ard_type in ard_types_grd:
                self.ard_type = ard_type
            else:
                raise ValueError(
                    "No valid ARD type for product type GRD."
                    f"Select from {ard_types_grd}"
                )

        # possible ard types to select from for GRD
        elif product_type == "SLC":

            ard_types_slc = [
                "OST-GTC",
                "OST-RTC",
                "OST-COH",
                "OST-RTCCOH",
                "OST-POL",
                "OST-ALL",
            ]

            if ard_type in ard_types_slc:
                self.ard_type = ard_type
            else:
                raise ValueError(
                    "No valid ARD type for product type GRD."
                    f"Select from {ard_types_slc}"
                )

        # otherwise the product type is not supported
        else:
            raise ValueError(
                f"Product type {self.product_type} not "
                f"supported for processing. Only GRD and SLC "
                f"are supported."
            )

        # ---------------------------------------
        # 2 Check beam mode
        if not beam_mode == "IW":
            raise ValueError("Only 'IW' beam mode supported for processing.")

        # ---------------------------------------
        # 3 Add snap_cpu_parallelism
        self.config_dict["snap_cpu_parallelism"] = snap_cpu_parallelism
        self.config_dict["max_workers"] = max_workers
        self.config_dict["executor_type"] = "billiard"

        # ---------------------------------------
        # 4 Set up project JSON
        self.config_file = self.project_dir / "config.json"
        self.ard_parameters = self.get_ard_parameters(ard_type)

        # re-create config dict with update ard parameters
        self.config_dict.update(processing=self.ard_parameters)

    # ---------------------------------------
    # methods
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

        # returning ard_parameters
        return self.ard_parameters

    def update_ard_parameters(self, ard_type=None):

        # if a ard type is selected, load
        if ard_type:
            self.get_ard_parameters(ard_type)

        # check for correctness of ard parameters
        check_ard_parameters(self.ard_parameters)

        # re-create project dict with update ard parameters
        self.config_dict.update(processing=self.ard_parameters)

        # dump to json file
        with open(self.config_file, "w") as outfile:
            json.dump(self.config_dict, outfile, indent=4)

    def set_external_dem(self, dem_file, ellipsoid_correction=True):

        # check if file exists
        if not Path(dem_file).exists():
            raise FileNotFoundError(f"No file found at {dem_file}.")

        # get no data value
        with rasterio.open(dem_file) as file:
            dem_nodata = int(file.nodata)

        # get resampling adn projection‚
        img_res = self.ard_parameters["single_ARD"]["dem"]["image_resampling"]
        dem_res = self.ard_parameters["single_ARD"]["dem"]["dem_resampling"]
        projection = self.ard_parameters["single_ARD"]["dem"]["out_projection"]

        # update ard parameters
        dem_dict = dict(
            {
                "dem_name": "External DEM",
                "dem_file": dem_file,
                "dem_nodata": dem_nodata,
                "dem_resampling": dem_res,
                "image_resampling": img_res,
                "egm_correction": ellipsoid_correction,
                "out_projection": projection,
            }
        )

        # update ard_parameters
        self.ard_parameters["single_ARD"]["dem"] = dem_dict

    def pre_download_srtm(self):

        logger.info("Pre-downloading SRTM tiles")
        srtm.download_srtm(self.aoi)

    def bursts_to_ards(
        self, timeseries=False, timescan=False, mosaic=False, overwrite=False
    ):
        """Batch processing function for full burst pre-processing workflow

        This function allows for the generation of the

        :param timeseries: if True, Time-series will be generated for
        each burst id
        :type timeseries: bool, optional
        :param timescan: if True, Timescans will be generated for each burst id
        type: timescan: bool, optional
        :param mosaic: if True, Mosaics will be generated from the
                       Time-Series/Timescans of each burst id
        :type mosaic: bool, optional
        :param overwrite: (if True, the processing folder will be
        emptied
        :type overwrite: bool, optional
        :param max_workers: number of parallel burst
        :type max_workers: int, default=1
        processing jobs
        :return:
        """

        # --------------------------------------------
        # 2 Check if within SRTM coverage
        # set ellipsoid correction and force GTC production
        # when outside SRTM
        center_lat = loads(self.aoi).centroid.y
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
        # 3 subset determination
        # we need a check function that checks
        self.config_dict["subset"] = False
        # This does not work at the moment, and maybe does not even make sense,
        # since for the co-registration we would need a sufficient
        # part of the image
        # self.config_dict['subset'] = vec.set_subset(
        #     self.aoi, self.burst_inventory
        # )

        # --------------------------------------------
        # 4 Check ard parameters in case they have been updated,
        #   and write them to json file
        self.update_ard_parameters()

        # --------------------------------------------
        # 1 delete data from previous runnings
        # delete data in temporary directory in case there is
        # something left from previous runs
        h.remove_folder_content(self.config_dict["temp_dir"])

        # in case we strat from scratch, delete all data
        # within processing folder
        if overwrite:
            logger.info("Deleting processing folder to start from scratch")
            h.remove_folder_content(self.config_dict["processing_dir"])

        # --------------------------------------------
        # 5 set resolution to degree
        # self.ard_parameters['resolution'] = h.resolution_in_degree(
        #    self.center_lat, self.ard_parameters['resolution'])

        # if self.config_dict['max_workers'] > 1 and self.ard_parameters['single_ARD']['dem']['dem_name'] == 'SRTM 1Sec HGT':
        #    self.pre_download_srtm()

        # --------------------------------------------
        # 6 run the burst to ard batch routine (up to 3 times if needed)
        i = 1
        while i < 4:
            processed_bursts_df = burst_batch.bursts_to_ards(
                self.burst_inventory, self.config_file
            )

            if False in processed_bursts_df.error.isnull().tolist():
                i += 1
            else:
                i = 5

        # write processed df to file
        processing_dir = Path(self.config_dict["processing_dir"])
        processed_bursts_df.to_pickle(processing_dir / "processed_bursts.pickle")

        # if not all have been processed, raise an error to avoid
        # false time-series processing
        if i == 4:
            raise RuntimeError("Not all all bursts have been successfully processed")

        # --------------------------------------------
        # 6 run the timeseries creation
        if timeseries or timescan:
            burst_batch.ards_to_timeseries(self.burst_inventory, self.config_file)

        # --------------------------------------------
        # 7 run the timescan creation
        if timescan:
            burst_batch.timeseries_to_timescan(self.burst_inventory, self.config_file)

        # --------------------------------------------
        # 8 mosaic the time-series
        if mosaic and timeseries:
            burst_batch.mosaic_timeseries(self.burst_inventory, self.config_file)

        # --------------------------------------------
        # 9 mosaic the timescans
        if mosaic and timescan:
            burst_batch.mosaic_timescan(self.burst_inventory, self.config_file)

        # return tseries_df

    @staticmethod
    def create_timeseries_animation(
        timeseries_dir,
        product_list,
        outfile,
        shrink_factor=1,
        resampling_factor=5,
        duration=1,
        add_dates=False,
        prefix=False,
    ):
        ras.create_timeseries_animation(
            timeseries_dir,
            product_list,
            outfile,
            shrink_factor=shrink_factor,
            duration=duration,
            resampling_factor=resampling_factor,
            add_dates=add_dates,
            prefix=prefix,
        )

    def grds_to_ards(
        self,
        inventory_df,
        timeseries=False,
        timescan=False,
        mosaic=False,
        overwrite=False,
        max_workers=1,
        executor_type="billiard",
    ):

        self.config_dict["max_workers"] = max_workers
        self.config_dict["executor_type"] = executor_type

        # in case we start from scratch, delete all data
        # within processing folder
        if overwrite:
            logger.info("Deleting processing folder to start from scratch")
            h.remove_folder_content(self.processing_dir)

        # --------------------------------------------
        # 2 Check if within SRTM coverage
        # set ellipsoid correction and force GTC production
        # when outside SRTM
        center_lat = loads(self.aoi).centroid.y
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
        # 3 subset determination
        # we need a check function that checks
        self.config_dict.update(subset=vec.set_subset(self.aoi, inventory_df))

        # dump to json file
        with open(self.config_file, "w") as outfile:
            json.dump(self.config_dict, outfile, indent=4)

        # --------------------------------------------
        # 4 Check ard parameters in case they have been updated,
        #   and write them to json file
        self.update_ard_parameters()

        # --------------------------------------------
        # 1 delete data in case of previous runs
        # delete data in temporary directory in case there is
        # something left from aborted previous runs
        h.remove_folder_content(self.config_dict["temp_dir"])

        # --------------------------------------------
        # 5 set resolution in degree
        # self.center_lat = loads(self.aoi).centroid.y
        # if float(self.center_lat) > 59 or float(self.center_lat) < -59:
        #   logger.info(
        #       'Scene is outside SRTM coverage. Will use 30m #
        #       'ASTER DEM instead.'
        #   )
        #   self.ard_parameters['dem'] = 'ASTER 1sec GDEM'

        # --------------------------------------------
        # 5 set resolution in degree
        # the grd to ard batch routine
        processing_df = grd_batch.grd_to_ard_batch(inventory_df, self.config_file)

        # time-series part
        if timeseries or timescan:
            grd_batch.ards_to_timeseries(inventory_df, self.config_file)

        if timescan:
            grd_batch.timeseries_to_timescan(inventory_df, self.config_file)

        if mosaic and timeseries:
            grd_batch.mosaic_timeseries(inventory_df, self.config_file)

        # --------------------------------------------
        # 9 mosaic the timescans
        if mosaic and timescan:
            grd_batch.mosaic_timescan(self.config_file)

        return processing_df
