import os
import geopandas as gpd
from tempfile import TemporaryDirectory

from ost.s1.search import scihub_catalogue
from ost.helpers.scihub import create_query, create_aoi_str, create_toi_str, \
    create_s1_product_specs
from ost.helpers.settings import HERBERT_USER


def test_default_scihub_catalogue():
    with TemporaryDirectory(dir=os.getcwd()) as temp:
        args_dict = {}
        args_dict.update(
            aoi='POLYGON ((16.875 45, 16.875 50.625, 11.25 50.625, 11.25 45, 16.875 45))',
            beammode='IW',
            begindate='2020-01-01',
            enddate='2020-01-04',
            output=os.path.join(temp, 'test_cat.shp'),
            password=HERBERT_USER['pword'],
            polarisation='VV,VH',
            producttype='GRD',
            username=HERBERT_USER['uname']
        )
        aoi = create_aoi_str(args_dict['aoi'])
        toi = create_toi_str(args_dict['begindate'], args_dict['enddate'])
        product_specs = create_s1_product_specs(args_dict['producttype'],
                                                args_dict['polarisation'],
                                                args_dict['beammode']
                                                )
        query_string = create_query('Sentinel-1', aoi, toi, product_specs)
        scihub_catalogue(
            query_string,
            output=args_dict['output'],
            append=False,
            uname=args_dict['username'],
            pword=args_dict['password']
        )
        control_fields = 21
        control_products = 34
        shp = gpd.read_file(args_dict['output'])
        assert len(shp.columns) == control_fields
        assert len(shp.id) == control_products
