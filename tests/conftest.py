import os
import pytest

from ost import Sentinel1_Scene

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPT_DIR, "testdata")
TEMP_DIR = os.path.join(TESTDATA_DIR, "tmp")
CACHE_DIR = os.path.join(TESTDATA_DIR, "cache")
FAKE_GPT_DIR = os.path.join(TESTDATA_DIR, "helpers_data")


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
def s1_grd_notnr_ost_product(s1_grd_notnr):
    scene_id = s1_grd_notnr.split('/')[-2]
    return (scene_id, Sentinel1_Scene(scene_id))


@pytest.fixture
def some_bounds():
    return [9.404296875, 54.84375, 9.4921875, 54.931640625]