import json
import pytest
import logging

from multiprocessing import cpu_count

from ost.s1 import slc_wrappers as sw
from ost.helpers.settings import OST_ROOT

logger = logging.getLogger(__name__)

# load standard config parameters
config_file = OST_ROOT.joinpath('graphs', 'ard_json', 'slc.ost_gtc.json')
with open(config_file, 'r') as file:
    CONFIG_DICT = json.load(file)
    CONFIG_DICT['subset'] = False
    CONFIG_DICT['snap_cpu_parallelism'] = cpu_count()
    CONFIG_DICT['max_workers'] = 1
    CONFIG_DICT['executor_type'] = 'billiard'


def test_burst_import(s1_slc_master, s1_slc_ost_master, slc_project_class):

    scene_id, master = s1_slc_ost_master
    for idx, burst in slc_project_class.burst_inventory.iterrows():
        if idx > 2 or burst.SwathID != 'IW1':
            continue
        return_code = sw.burst_import(
            infile=s1_slc_master,
            outfile=slc_project_class.processing_dir.joinpath(
                f'{scene_id}_{burst.bid}_import'
            ),
            logfile=logger,
            swath=burst.SwathID,
            burst=burst.BurstNr,
            config_dict=CONFIG_DICT
        )
        assert return_code == str(
            slc_project_class.processing_dir.joinpath(
                f'{scene_id}_{burst.bid}_import.dim'
            )
        )


#@pytest.mark.skip(reason="Some GPT Error, but does not happen in production!")
def test_burst_calibration(s1_slc_ost_master, slc_project_class):

    scene_id, master = s1_slc_ost_master
    for idx, burst in slc_project_class.burst_inventory.iterrows():
        if idx > 2 or burst.SwathID != 'IW1':
            continue
        return_code = sw.calibration(
            infile=slc_project_class.processing_dir.joinpath(
                f'{scene_id}_{burst.bid}_import.dim'
            ),
            outfile=slc_project_class.processing_dir.joinpath(
                f'{scene_id}_BS'
            ),
            logfile=logger,
            config_dict=CONFIG_DICT
        )
        assert return_code == str(
            slc_project_class.processing_dir.joinpath(
                f'{scene_id}_BS.dim'
            )
        )


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
        return_code = sw.ha_alpha(
            infile=s1_slc_master,
            outfile=slc_project_class.processing_dir.joinpath(
                f'{scene_id}_ha_alpha'
            ),
            logfile=logger,
            config_dict=CONFIG_DICT
        )
        assert return_code == str(
            slc_project_class.processing_dir.joinpath(
                f'{scene_id}_ha_alpha.dim'
            )
        )
