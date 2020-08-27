import pytest
import shutil
from pathlib import Path
from shapely.geometry import box

from ost.Project import Sentinel1Batch
from ost.s1.s1scene import Sentinel1Scene
from ost.helpers.settings import HERBERT_USER

SCRIPT_DIR = Path(__file__).resolve().parent
TESTDATA_DIR = SCRIPT_DIR.joinpath('testdata')
TEMP_SLC_DIR = TESTDATA_DIR.joinpath('tmp_slc')
TEMP_GRD_DIR = TESTDATA_DIR.joinpath('tmp_grd')
CACHE_DIR = TESTDATA_DIR.joinpath('cache')


@pytest.fixture(scope='session')
def some_bounds_grd():
    return [9.404296875, 54.84375, 9.4921875, 54.931640625]


@pytest.fixture(scope='session')
def some_bounds_slc():
    return [8.02001953125, 46.34033203125, 8.0419921875, 46.3623046875]


@pytest.fixture
def s1_id():
    return (
        'S1A_IW_GRDH_1SDV_20141003T040550_20141003T040619_002660_002F64_EC04'
    )


@pytest.fixture(scope='session')
def s1_grd_notnr():
    return CACHE_DIR.joinpath(
        'S1B_IW_GRDH_1SDV_20180813T054020_20180813T054045_012240_0168D6_B775'
        '.zip'
    )


@pytest.fixture(scope='session')
def s1_grd_notnr_ost_product(s1_grd_notnr):
    scene_id = s1_grd_notnr.stem
    return scene_id, Sentinel1Scene(scene_id)


@pytest.fixture(scope='session')
def s1_slc_master():
    return CACHE_DIR.joinpath(
        'S1A_IW_SLC__1SDV_20190101T171515_20190101T171542_'
        '025287_02CC09_0A0B.zip'
    )


@pytest.fixture
def s1_slc_slave():
    return CACHE_DIR.joinpath(
        'S1A_IW_SLC__1SDV_20190113T171514_20190113T171541_025462_02D252_C063'
        '.zip'
    )


@pytest.fixture(scope='session')
def s1_slc_ost_master(s1_slc_master):
    scene_id = s1_slc_master.stem
    return scene_id, Sentinel1Scene(scene_id)


@pytest.fixture
def s1_slc_ost_slave(s1_slc_slave):
    scene_id = s1_slc_slave.stem
    return scene_id, Sentinel1Scene(scene_id)


@pytest.fixture(scope='session')
def slc_project_class(some_bounds_slc, s1_slc_master, s1_slc_ost_master):
    start = '2019-01-01'
    end = '2019-01-02'
    scene_id, product = s1_slc_ost_master
    TEMP_SLC_DIR.mkdir(parents=True, exist_ok=True)
    aoi = box(
        some_bounds_slc[0], some_bounds_slc[1], some_bounds_slc[2],
        some_bounds_slc[3]
    ).wkt

    try:
        s1_batch = Sentinel1Batch(
            project_dir=TEMP_SLC_DIR,
            aoi=aoi,
            start=start,
            end=end,
            product_type='SLC',
            ard_type='OST-RTC',
        )

        download_path = s1_batch.download_dir.joinpath(
            'SAR',
            product.product_type,
            product.year,
            product.month,
            product.day
        )

        product.download_path(download_dir=download_path)
        download_path.mkdir(parents=True, exist_ok=True)
        shutil.copy(s1_slc_master, download_path)
        shutil.move(
            download_path.joinpath(f'{scene_id}.zip'),
            download_path.joinpath(f'{scene_id}.downloaded')
        )
        shutil.copy(s1_slc_master, download_path)
        product.get_path(download_dir=s1_batch.download_dir)

        s1_batch.scihub_uname = HERBERT_USER['uname']
        s1_batch.scihub_pword = HERBERT_USER['pword']
        s1_batch.search()

        s1_batch.refine_inventory()
        s1_batch.create_burst_inventory()

        yield s1_batch
    finally:
        shutil.rmtree(TEMP_SLC_DIR)


@pytest.fixture(scope='session')
def grd_project_class(some_bounds_grd, s1_grd_notnr, s1_grd_notnr_ost_product):

    start = '2018-08-13'
    end = '2018-08-13'

    scene_id, product = s1_grd_notnr_ost_product
    TEMP_GRD_DIR.mkdir(parents=True, exist_ok=True)

    aoi = box(
        some_bounds_grd[0], some_bounds_grd[1], some_bounds_grd[2],
        some_bounds_grd[3]
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
        download_path = s1_batch.download_dir.joinpath(
            'SAR',
            product.product_type,
            product.year,
            product.month,
            product.day
        )
        product.download_path(download_dir=download_path)
        download_path.mkdir(parents=True, exist_ok=True)
        shutil.copy(s1_grd_notnr, download_path)
        shutil.move(
            download_path.joinpath(f'{scene_id}.zip'),
            download_path.joinpath(f'{scene_id}.downloaded')
        )
        shutil.copy(s1_grd_notnr, download_path)
        product.get_path(download_dir=s1_batch.download_dir)

        s1_batch.scihub_uname = HERBERT_USER['uname']
        s1_batch.scihub_pword = HERBERT_USER['pword']
        s1_batch.search()

        s1_batch.refine_inventory()
        yield s1_batch
    finally:
        shutil.rmtree(TEMP_GRD_DIR)
