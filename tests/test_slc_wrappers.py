import os
import logging

from ost.s1.slc_wrappers import _import, _calibration, \
    _coreg2, _coherence, _ha_alpha

logger = logging.getLogger(__name__)


def test_burst_import(s1_slc_master,
                      s1_slc_ost_master,
                      master_project_class
                      ):
    scene_id, master = s1_slc_ost_master
    for idx, burst in master_project_class.burst_inventory.iterrows():
        if idx > 2:
            continue
        return_code = _import(
            infile=s1_slc_master,
            out_prefix=os.path.join(
                master_project_class.processing_dir, scene_id+'_import'
            ),
            logfile=logger,
            swath=burst.SwathID,
            burst=burst.BurstNr,
            polar='VV,VH,HH,HV',
            ncores=os.cpu_count()
        )
        assert return_code == 0


def test_burst_calibration(s1_slc_ost_master,
                           master_project_class,
                           ):
    scene_id, master = s1_slc_ost_master
    for idx, burst in master_project_class.burst_inventory.iterrows():
        if idx > 2:
            continue
        return_code = _calibration(
            infile=os.path.join(
                master_project_class.processing_dir, scene_id+'_import.dim'
            ),
            outfile=scene_id+'_BS',
            logfile=logger,
            product_type=master_project_class.ard_parameters['single ARD']
            ['product type'],
            ncores=os.cpu_count()
        )
        assert return_code == 0


def test_burst_ha_alpha(
        s1_slc_master,
        s1_slc_ost_master,
        master_project_class,
):
    scene_id, master = s1_slc_ost_master
    for idx, burst in master_project_class.burst_inventory.iterrows():
        if idx > 2:
            continue
        return_code = _ha_alpha(
            infile=s1_slc_master,
            outfile=os.path.join(
                master_project_class.processing_dir, scene_id+'_ha_alpha'
            ),
            logfile=logger,
            # pol_speckle_filter=master_project_class.ard_parameters
            # ['single ARD']['remove pol speckle'],
            pol_speckle_filter=False,
            pol_speckle_dict=master_project_class.ard_parameters
            ['single ARD']['pol speckle filter'],
            ncores=os.cpu_count()
        )
        assert return_code == 0
