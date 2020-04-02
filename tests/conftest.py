import os
import pytest
import shutil
from shapely.geometry import box

from ost.Project import Sentinel1Batch
from ost.s1.s1scene import Sentinel1Scene
from ost.helpers.settings import HERBERT_USER

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPT_DIR, "testdata")
TEMP_SLC_DIR = os.path.join(TESTDATA_DIR, "tmp_slc")
TEMP_GRD_DIR = os.path.join(TESTDATA_DIR, "tmp_grd")
CACHE_DIR = os.path.join(TESTDATA_DIR, "cache")


@pytest.fixture(scope='session')
def some_bounds_grd():
    return [9.404296875, 54.84375, 9.4921875, 54.931640625]


@pytest.fixture(scope='session')
def some_bounds_slc():
    return [8.02001953125, 46.34033203125, 8.0419921875, 46.3623046875]


@pytest.fixture
def s1_id():
    return 'S1A_IW_GRDH_1SDV_20141003T040550_20141003T040619_002660_002F64_EC04'


@pytest.fixture(scope='session')
def s1_grd_notnr():
    return os.path.join(
        CACHE_DIR,
        'S1B_IW_GRDH_1SDV_20180813T054020_20180813T054045_012240_0168D6_B775.zip'
    )


@pytest.fixture(scope='session')
def s1_grd_notnr_ost_product(s1_grd_notnr):
    scene_id = os.path.basename(s1_grd_notnr).replace('.zip', '')
    return (scene_id, Sentinel1Scene(scene_id))


@pytest.fixture(scope='session')
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


@pytest.fixture(scope='session')
def s1_slc_ost_master(s1_slc_master):
    scene_id = os.path.basename(s1_slc_master).replace('.zip', '')
    return (scene_id, Sentinel1Scene(scene_id))


@pytest.fixture
def s1_slc_ost_slave(s1_slc_slave):
    scene_id = os.path.basename(s1_slc_slave).replace('.zip', '')
    return (scene_id, Sentinel1Scene(scene_id))


@pytest.fixture(scope='session')
def slc_project_class(some_bounds_slc, s1_slc_master, s1_slc_ost_master):
    start = '2019-01-01'
    end = '2019-01-02'
    scene_id, product = s1_slc_ost_master
    os.makedirs(TEMP_SLC_DIR, exist_ok=True)
    aoi = box(some_bounds_slc[0], some_bounds_slc[1],
              some_bounds_slc[2], some_bounds_slc[3]
              ).wkt
    try:
        s1_batch = Sentinel1Batch(
            project_dir=TEMP_SLC_DIR,
            aoi=aoi,
            start=start,
            end=end,
            product_type='SLC',
            ard_type='OST-RTC'
        )
        download_path = os.path.join(s1_batch.download_dir,
                                     'SAR',
                                     product.product_type,
                                     product.year,
                                     product.month,
                                     product.day
                                     )
        os.makedirs(download_path, exist_ok=True)
        shutil.copy(s1_slc_master, download_path)
        shutil.move(
            os.path.join(download_path, scene_id+'.zip'),
            os.path.join(download_path, scene_id+'.zip.downloaded')
        )
        shutil.copy(s1_slc_master, download_path)
        product.get_path(download_dir=s1_batch.download_dir)
        s1_batch.search(uname=HERBERT_USER['uname'],
                        pword=HERBERT_USER['pword']
                        )
        s1_batch.refine()
        s1_batch.create_burst_inventory(key='ASCENDING_VVVH',
                                        uname=HERBERT_USER['uname'],
                                        pword=HERBERT_USER['pword']
                                        )

        yield s1_batch
    finally:
        shutil.rmtree(TEMP_SLC_DIR)


@pytest.fixture(scope='session')
def grd_project_class(some_bounds_grd, s1_grd_notnr, s1_grd_notnr_ost_product):
    start = '2018-08-13'
    end = '2018-08-14'
    scene_id, product = s1_grd_notnr_ost_product
    os.makedirs(TEMP_GRD_DIR, exist_ok=True)
    aoi = box(some_bounds_grd[0], some_bounds_grd[1],
              some_bounds_grd[2], some_bounds_grd[3]
              ).wkt
    try:
        s1_batch = Sentinel1Batch(
            project_dir=TEMP_GRD_DIR,
            aoi=aoi,
            start=start,
            end=end,
            product_type='GRD',
            ard_type='OST-RTC'
        )
        download_path = os.path.join(s1_batch.download_dir,
                                     'SAR',
                                     product.product_type,
                                     product.year,
                                     product.month,
                                     product.day
                                     )
        os.makedirs(download_path, exist_ok=True)
        shutil.copy(s1_grd_notnr, download_path)
        shutil.move(
            os.path.join(download_path, scene_id+'.zip'),
            os.path.join(download_path, scene_id+'.zip.downloaded')
        )
        shutil.copy(s1_grd_notnr, download_path)
        product.get_path(download_dir=s1_batch.download_dir)
        s1_batch.search(uname=HERBERT_USER['uname'],
                        pword=HERBERT_USER['pword']
                        )
        s1_batch.refine()
        yield s1_batch
    finally:
        shutil.rmtree(TEMP_GRD_DIR)