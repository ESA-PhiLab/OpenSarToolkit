import os
import pytest

from ost.s1.s1scene import Sentinel1_Scene

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPT_DIR, "testdata")
TEMP_DIR = os.path.join(TESTDATA_DIR, "tmp")
CACHE_DIR = os.path.join(TESTDATA_DIR, "cache")


@pytest.fixture
def s1_id():
    return 'S1A_IW_GRDH_1SDV_20141003T040550_20141003T040619_002660_002F64_EC04'


@pytest.fixture
def s1_grd_notnr():
    return os.path.join(
        CACHE_DIR,
        'S1B_IW_GRDH_1SDV_20180813T054020_20180813T054045_012240_0168D6_B775'
    )


@pytest.fixture
def s1_grd_notnr_ost_product(s1_grd_notnr):
    scene_id = s1_grd_notnr.split('/')[-1]
    return (scene_id, Sentinel1_Scene(scene_id))
