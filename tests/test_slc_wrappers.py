import os
import pytest
import logging

from ost.s1.slc_wrappers import burst_import, calibration, \
    coreg2, coherence, ha_alpha

logger = logging.getLogger(__name__)


def test_burst_import(s1_slc_master,
                      s1_slc_ost_master,
                      slc_project_class
                      ):
    scene_id, master = s1_slc_ost_master
    for idx, burst in slc_project_class.burst_inventory.iterrows():
        if idx > 2 or burst.SwathID != 'IW1':
            continue
        return_code = burst_import(
            infile=s1_slc_master,
            outfile=os.path.join(
                slc_project_class.processing_dir,
                scene_id+'_'+burst.bid+'_import'
            ),
            logfile=logger,
            swath=burst.SwathID,
            burst=burst.BurstNr,
            polar='VV,VH,HH,HV',
            ncores=os.cpu_count()
        )
        assert return_code == 0


def test_burst_calibration(s1_slc_ost_master,
                           slc_project_class,
                           ):
    ard_params = {
        'dem': {
                "dem_name": "SRTM 1Sec HGT",
                "dem_file": "",
                "dem_nodata": 0,
                "dem_resampling": "BILINEAR_INTERPOLATION",
                "image_resampling": "BICUBIC_INTERPOLATION",
                "egm_correction": False,
                "out_projection": "WGS84(DD)"
            },
        'product_type': 'RTC-gamma0'
    }
    scene_id, master = s1_slc_ost_master
    for idx, burst in slc_project_class.burst_inventory.iterrows():
        if idx > 2 or burst.SwathID != 'IW1':
            continue
        return_code = calibration(
            infile=os.path.join(
                slc_project_class.processing_dir,
                scene_id+'_'+burst.bid+'_import.dim'
            ),
            outfile=os.path.join(
                slc_project_class.processing_dir, scene_id+'_BS'
            ),
            logfile=logger,
            ard=ard_params,
            region=slc_project_class.aoi,
            ncores=os.cpu_count())

        assert return_code == 0


@pytest.mark.skip(reason="Takes too long skip for now!")
def test_burst_ha_alpha(
        s1_slc_master,
        s1_slc_ost_master,
        slc_project_class,
):
    scene_id, master = s1_slc_ost_master
    for idx, burst in slc_project_class.burst_inventory.iterrows():
        if idx > 2:
            continue
        return_code = ha_alpha(
            infile=s1_slc_master,
            outfile=os.path.join(
                slc_project_class.processing_dir, scene_id+'_ha_alpha'
            ),
            logfile=logger,
            # pol_speckle_filter=slc_project_class.ard_parameters
            # ['single ARD']['remove pol speckle'],
            pol_speckle_filter=False,
            pol_speckle_dict=slc_project_class.ard_parameters
            ['single ARD']['pol speckle filter'],
            ncores=os.cpu_count()
        )
        assert return_code == 0
