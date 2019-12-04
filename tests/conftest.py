import os
import pytest

from ost import Sentinel1Scene

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPT_DIR, "testdata")
TEMP_DIR = os.path.join(TESTDATA_DIR, "tmp")
CACHE_DIR = os.path.join(TESTDATA_DIR, "cache")
FAKE_GPT_DIR = os.path.join(TESTDATA_DIR, "helpers_data")


@pytest.fixture
def s1_id():
    return 'S1A_IW_GRDH_1SDV_20191116T170638_20191116T170703_029939_036AAB_070F'


@pytest.fixture
def ard_types():
    return ('OST', 'OST Flat', 'CEOS', 'EarthEngine', 'Zhuo')


@pytest.fixture
def s1_grd_notnr():
    return os.path.join(
        CACHE_DIR,
        'S1B_IW_GRDH_1SDV_20180813T054020_20180813T054045_012240_0168D6_B775',
        'manifest.safe'
    )


@pytest.fixture
def s1_slc_master():
    return os.path.join(
        CACHE_DIR,
        'S1A_IW_SLC__1SDV_20190101T171515_20190101T171542_025287_02CC09_0A0B.zip',
     )


@pytest.fixture
def s1_slc_slave():
    return os.path.join(
        CACHE_DIR,
        'S1A_IW_SLC__1SDV_20190113T171514_20190113T171541_025462_02D252_C063.zip',
    )


@pytest.fixture
def s1_slc_ost_master(s1_slc_master):
    scene_id = os.path.basename(s1_slc_master).replace('.zip', '')
    return (scene_id, Sentinel1Scene(scene_id))


@pytest.fixture
def s1_slc_ost_slave(s1_slc_slave):
    scene_id = os.path.basename(s1_slc_slave).replace('.zip', '')
    return (scene_id, Sentinel1Scene(scene_id))


@pytest.fixture
def s1_grd_notnr_ost_product(s1_grd_notnr):
    scene_id = s1_grd_notnr.split('/')[-2]
    return (scene_id, Sentinel1Scene(scene_id))


@pytest.fixture
def some_bounds():
    return [9.404296875, 54.84375, 9.4921875, 54.931640625]


@pytest.fixture
def some_bounds_slc():
    return [8.02001953125, 46.34033203125, 8.0419921875, 46.3623046875]