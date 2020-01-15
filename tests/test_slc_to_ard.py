import os
import numpy as np
import rasterio
from shapely.geometry import box
from tempfile import TemporaryDirectory

from ost.helpers.helpers import _slc_zip_to_processing_dir


def test_ost_slc_to_ard(
        ard_types,
        s1_slc_master,
        s1_slc_ost_master,
        some_bounds_slc
):
    ard_type = ard_types[0]
    out_bounds = str(box(some_bounds_slc[0],
                         some_bounds_slc[1],
                         some_bounds_slc[2],
                         some_bounds_slc[3])
                     )
    with TemporaryDirectory(dir=os.getcwd()) as processing_dir, \
            TemporaryDirectory() as temp:
        scene_id, product = s1_slc_ost_master
        # Make Creodias-like paths
        download_path = os.path.join(processing_dir, 'SAR',
                                     product.product_type,
                                     product.year,
                                     product.month,
                                     product.day
                                     )
        os.makedirs(download_path, exist_ok=True)
        _slc_zip_to_processing_dir(
            processing_dir=processing_dir,
            product=product,
            product_path=s1_slc_master
        )
        product.set_ard_parameters(ard_type)
        try:
            out_files = product.create_ard(
                infile=product.get_path(processing_dir),
                out_dir=processing_dir,
                out_prefix=scene_id+'_'+ard_type,
                temp_dir=temp,
                subset=out_bounds,
            )
        except Exception as e:
            raise e
        for f in out_files:
            assert os.path.isfile(f)
        product.create_rgb(
            outfile=os.path.join(processing_dir, scene_id+'_'+ard_type+'.tif')
        )
        tif_path = product.ard_rgb
        with rasterio.open(tif_path, 'r') as out_tif:
            raster_sum = np.nansum(out_tif.read(1))
        assert raster_sum != 0


def test_ost_flat_slc_to_ard(
        ard_types,
        s1_slc_master,
        s1_slc_ost_master,
        some_bounds_slc
):
    ard_type = ard_types[1]
    out_bounds = str(box(some_bounds_slc[0],
                         some_bounds_slc[1],
                         some_bounds_slc[2],
                         some_bounds_slc[3])
                     )
    with TemporaryDirectory(dir=os.getcwd()) as processing_dir, \
            TemporaryDirectory() as temp:
        scene_id, product = s1_slc_ost_master
        # Make Creodias-like paths
        download_path = os.path.join(processing_dir, 'SAR',
                                     product.product_type,
                                     product.year,
                                     product.month,
                                     product.day
                                     )
        os.makedirs(download_path, exist_ok=True)
        _slc_zip_to_processing_dir(
            processing_dir=processing_dir,
            product=product,
            product_path=s1_slc_master
        )
        product.set_ard_parameters(ard_type)
        try:
            out_files = product.create_ard(
                infile=product.get_path(processing_dir),
                out_dir=processing_dir,
                out_prefix=scene_id+'_'+ard_type,
                temp_dir=temp,
                subset=out_bounds,
            )
        except Exception as e:
            raise e
        for f in out_files:
            assert os.path.isfile(f)
        product.create_rgb(
            outfile=os.path.join(processing_dir, scene_id+'_'+ard_type+'.tif')
        )
        tif_path = product.ard_rgb
        with rasterio.open(tif_path, 'r') as out_tif:
            raster_sum = np.nansum(out_tif.read(1))
        assert raster_sum != 0


def test_earth_engine_slc_to_ard(
        ard_types,
        s1_slc_master,
        s1_slc_ost_master,
        some_bounds_slc
):
    ard_type = ard_types[3]
    out_bounds = str(box(some_bounds_slc[0],
                         some_bounds_slc[1],
                         some_bounds_slc[2],
                         some_bounds_slc[3])
                     )
    with TemporaryDirectory(dir=os.getcwd()) as processing_dir, \
            TemporaryDirectory() as temp:
        scene_id, product = s1_slc_ost_master
        # Make Creodias-like paths
        download_path = os.path.join(processing_dir, 'SAR',
                                     product.product_type,
                                     product.year,
                                     product.month,
                                     product.day
                                     )
        os.makedirs(download_path, exist_ok=True)
        _slc_zip_to_processing_dir(
            processing_dir=processing_dir,
            product=product,
            product_path=s1_slc_master
        )
        product.set_ard_parameters(ard_type)
        try:
            out_files = product.create_ard(
                infile=product.get_path(processing_dir),
                out_dir=processing_dir,
                out_prefix=scene_id+'_'+ard_type,
                temp_dir=temp,
                subset=out_bounds,
            )
        except Exception as e:
            raise e
        for f in out_files:
            assert os.path.isfile(f)
        product.create_rgb(
            outfile=os.path.join(processing_dir, scene_id+'_'+ard_type+'.tif')
        )
        tif_path = product.ard_rgb
        with rasterio.open(tif_path, 'r') as out_tif:
            raster_sum = np.nansum(out_tif.read(1))
        assert raster_sum != 0


def test_ceos_slc_to_ard(
        ard_types,
        s1_slc_master,
        s1_slc_ost_master,
        some_bounds_slc
):
    ard_type = ard_types[2]
    out_bounds = str(box(some_bounds_slc[0],
                         some_bounds_slc[1],
                         some_bounds_slc[2],
                         some_bounds_slc[3])
                     )
    with TemporaryDirectory(dir=os.getcwd()) as processing_dir, \
            TemporaryDirectory() as temp:
        scene_id, product = s1_slc_ost_master
        # Make Creodias-like paths
        download_path = os.path.join(processing_dir, 'SAR',
                                     product.product_type,
                                     product.year,
                                     product.month,
                                     product.day
                                     )
        os.makedirs(download_path, exist_ok=True)
        _slc_zip_to_processing_dir(
            processing_dir=processing_dir,
            product=product,
            product_path=s1_slc_master
        )
        product.set_ard_parameters(ard_type)
        try:
            out_files = product.create_ard(
                infile=product.get_path(processing_dir),
                out_dir=processing_dir,
                out_prefix=scene_id+'_'+ard_type,
                temp_dir=temp,
                subset=out_bounds,
            )
        except Exception as e:
            raise e
        for f in out_files:
            assert os.path.isfile(f)
        product.create_rgb(
            outfile=os.path.join(processing_dir, scene_id+'_'+ard_type+'.tif')
        )
        tif_path = product.ard_rgb
        with rasterio.open(tif_path, 'r') as out_tif:
            raster_sum = np.nansum(out_tif.read(1))
        assert raster_sum != 0


def test_zhuo_slc_to_ard(
        ard_types,
        s1_slc_master,
        s1_slc_ost_master,
        some_bounds_slc
):
    ard_type = ard_types[4]
    out_bounds = str(box(some_bounds_slc[0],
                         some_bounds_slc[1],
                         some_bounds_slc[2],
                         some_bounds_slc[3])
                     )
    with TemporaryDirectory(dir=os.getcwd()) as processing_dir, \
            TemporaryDirectory() as temp:
        scene_id, product = s1_slc_ost_master
        # Make Creodias-like paths
        download_path = os.path.join(processing_dir, 'SAR',
                                     product.product_type,
                                     product.year,
                                     product.month,
                                     product.day
                                     )
        os.makedirs(download_path, exist_ok=True)
        _slc_zip_to_processing_dir(
            processing_dir=processing_dir,
            product=product,
            product_path=s1_slc_master
        )
        product.set_ard_parameters(ard_type)
        try:
            out_files = product.create_ard(
                infile=product.get_path(processing_dir),
                out_dir=processing_dir,
                out_prefix=scene_id+'_'+ard_type,
                temp_dir=temp,
                subset=out_bounds,
            )
        except Exception as e:
            raise e
        for f in out_files:
            assert os.path.isfile(f)
        product.create_rgb(
            outfile=os.path.join(processing_dir, scene_id+'_'+ard_type+'.tif')
        )
        tif_path = product.ard_rgb
        with rasterio.open(tif_path, 'r') as out_tif:
            raster_sum = np.nansum(out_tif.read(1))
        assert raster_sum != 0
