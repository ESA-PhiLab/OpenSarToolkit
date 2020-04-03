import os
import glob
import pytest
import logging

from ost.generic.common_wrappers import terrain_correction, \
    speckle_filter, multi_look, linear_to_db, ls_mask, calibration
from ost.s1.grd_to_ard import _grd_frame_import_subset, _grd_remove_border

logger = logging.getLogger(__name__)


def test_grd_import_subset(s1_grd_notnr,
                           s1_grd_notnr_ost_product,
                           grd_project_class
                           ):
    scene_id, product = s1_grd_notnr_ost_product
    return_code = _grd_frame_import_subset(
        infile=s1_grd_notnr,
        outfile=os.path.join(
            grd_project_class.processing_dir,
            scene_id+'_import'
        ),
        georegion=grd_project_class.aoi,
        logfile=logger,
        polarisation='VV,VH,HH,HV'
    )
    assert return_code == 0


def test_grd_remove_border(s1_grd_notnr_ost_product,
                           grd_project_class
                           ):
    scene_id, product = s1_grd_notnr_ost_product
    for polarisation in ['VV', 'VH', 'HH', 'HV']:
        infile = glob.glob(os.path.join(
            grd_project_class.processing_dir,
            '{}_imported*data'.format(scene_id),
            'Intensity_{}.img'.format(polarisation))
        )
        if len(infile) == 1:
            # run grd Border Remove
            logger.debug('Remove border noise for {} band.'.format(
                polarisation))
            _grd_remove_border(infile[0])


def test_grd_calibration(s1_grd_notnr_ost_product,
                         grd_project_class
                         ):
    scene_id, product = s1_grd_notnr_ost_product
    calib_list = ['beta0', 'sigma0', 'gamma0']
    for calib in calib_list:
        return_code = calibration(
            infile=os.path.join(
                grd_project_class.processing_dir,
                scene_id+'_import.dim'
            ),
            outfile=os.path.join(
                grd_project_class.processing_dir,
                scene_id+'_BS.dim'
            ),
            logfile=logger,
            calibrate_to=calib,
            ncores=os.cpu_count()
        )
        assert return_code == 0


def test_grd_speckle_filter(s1_grd_notnr_ost_product,
                            grd_project_class
                            ):
    scene_id, product = s1_grd_notnr_ost_product
    return_code = speckle_filter(
        infile=os.path.join(
            grd_project_class.processing_dir,
            scene_id+'_BS.dim'
        ),
        outfile=os.path.join(
            grd_project_class.processing_dir,
            scene_id+'_BS_Spk.dim'
        ),
        logfile=logger,
        speckle_dict=grd_project_class.ard_parameters
        ['single_ARD']['speckle_filter'],
        ncores=os.cpu_count()
    )
    assert return_code == 0


def test_grd_tc(s1_grd_notnr_ost_product,
                grd_project_class
                ):
    scene_id, product = s1_grd_notnr_ost_product
    return_code = terrain_correction(
        infile=os.path.join(
            grd_project_class.processing_dir,
            scene_id+'_BS_Spk.dim'
        ),
        outfile=os.path.join(
            grd_project_class.processing_dir,
            scene_id+'_BS_Spk_TC.dim'
        ),
        logfile=logger,
        resolution=grd_project_class.ard_parameters
        ['single_ARD']['resolution'],
        dem_dict=grd_project_class.ard_parameters
        ['single_ARD']['dem'],
        ncores=os.cpu_count()
    )
    assert return_code == 0