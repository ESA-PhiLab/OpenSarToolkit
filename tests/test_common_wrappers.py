import json
import logging
import pytest

from multiprocessing import cpu_count

from ost.helpers.settings import OST_ROOT
from ost.generic import common_wrappers as cw
from ost.s1 import grd_wrappers as gw

logger = logging.getLogger(__name__)

# load standard config parameters
config_file = OST_ROOT.joinpath("graphs", "ard_json", "grd.ost_gtc.json")
with open(config_file, "r") as file:
    CONFIG_DICT = json.load(file)
    CONFIG_DICT["snap_cpu_parallelism"] = cpu_count()
    CONFIG_DICT["max_workers"] = 1
    CONFIG_DICT["executor_type"] = "billiard"


@pytest.mark.skip(reason="not running in pip build")
def test_grd_import_subset(s1_grd_notnr, s1_grd_notnr_ost_product, grd_project_class):

    # set subset
    CONFIG_DICT["aoi"] = grd_project_class.aoi
    CONFIG_DICT["subset"] = True

    scene_id, product = s1_grd_notnr_ost_product
    return_code = gw.grd_frame_import(
        infile=s1_grd_notnr,
        outfile=grd_project_class.processing_dir.joinpath(f"{scene_id}_import"),
        logfile=logger,
        config_dict=CONFIG_DICT,
    )
    assert return_code == str(
        grd_project_class.processing_dir.joinpath(f"{scene_id}_import.dim")
    )


@pytest.mark.skip(reason="not running in pip build")
def test_grd_remove_border(s1_grd_notnr_ost_product, grd_project_class):

    scene_id, product = s1_grd_notnr_ost_product
    for polarisation in ["VV", "VH", "HH", "HV"]:
        infile = list(
            grd_project_class.processing_dir.joinpath(f"{scene_id}_imported*data").glob(
                f"Intensity_{polarisation}.img"
            )
        )

        if len(infile) == 1:
            # run grd Border Remove
            logger.debug(f"Remove border noise for {polarisation} band.")
            gw.grd_remove_border(infile[0])


@pytest.mark.skip(reason="not running in pip build")
def test_grd_calibration(s1_grd_notnr_ost_product, grd_project_class):

    scene_id, product = s1_grd_notnr_ost_product
    product_types = ["GTC-gamma0", "GTC-sigma0", "RTC-gamma0"]

    for product_type in product_types:

        # set product type
        CONFIG_DICT["processing"]["single_ARD"]["product_type"] = product_type

        # run command
        return_code = gw.calibration(
            infile=grd_project_class.processing_dir.joinpath(f"{scene_id}_import.dim"),
            outfile=grd_project_class.processing_dir.joinpath(f"{scene_id}_BS"),
            logfile=logger,
            config_dict=CONFIG_DICT,
        )
        assert return_code == str(
            grd_project_class.processing_dir.joinpath(f"{scene_id}_BS.dim")
        )


@pytest.mark.skip(reason="not running in pip build")
def test_grd_speckle_filter(s1_grd_notnr_ost_product, grd_project_class):

    CONFIG_DICT["processing"]["single_ARD"]["remove_speckle"] = True
    scene_id, product = s1_grd_notnr_ost_product
    return_code = cw.speckle_filter(
        infile=grd_project_class.processing_dir.joinpath(f"{scene_id}_BS.dim"),
        outfile=grd_project_class.processing_dir.joinpath(f"{scene_id}_BS_Spk"),
        logfile=logger,
        config_dict=CONFIG_DICT,
    )
    assert return_code == str(
        grd_project_class.processing_dir.joinpath(f"{scene_id}_BS_Spk.dim")
    )


@pytest.mark.skip(reason="not running in pip build")
def test_grd_tc(s1_grd_notnr_ost_product, grd_project_class):

    scene_id, product = s1_grd_notnr_ost_product
    return_code = cw.terrain_correction(
        infile=grd_project_class.processing_dir.joinpath(f"{scene_id}_BS_Spk.dim"),
        outfile=grd_project_class.processing_dir.joinpath(f"{scene_id}_BS_Spk_TC"),
        logfile=logger,
        config_dict=CONFIG_DICT,
    )
    assert return_code == str(
        grd_project_class.processing_dir.joinpath(f"{scene_id}_BS_Spk_TC.dim")
    )
