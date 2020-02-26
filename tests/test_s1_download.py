import os
import pytest
import pandas as pd
from tempfile import TemporaryDirectory

from ost.helpers.helpers import check_zipfile
from ost.helpers.asf import check_connection as check_connection_asf
from ost.helpers.scihub import check_connection as check_connection_scihub, \
    connect
from ost.s1.download import download_sentinel1

from ost.settings import HERBERT_USER


def test_asf_connection():
    herbert_uname = HERBERT_USER['uname']
    herbert_password = HERBERT_USER['asf_pword']
    response_code = check_connection_asf(uname=herbert_uname,
                                         pword=herbert_password
                                         )
    control_code = 200
    assert response_code == control_code


def test_esa_scihub_connection(s1_grd_notnr_ost_product):
    herbert_uname = HERBERT_USER['uname']
    herbert_password = HERBERT_USER['pword']
    response_code = check_connection_scihub(uname=herbert_uname,
                                            pword=herbert_password
                                            )
    control_code = 200
    assert response_code == control_code
    opener = connect(
        base_url='https://scihub.copernicus.eu/apihub/',
        uname=herbert_uname,
        pword=herbert_password
    )
    control_uuid = '1b43fb7d-bd2c-41cd-86a1-3442b1fbd5bb'
    uuid = s1_grd_notnr_ost_product[1].scihub_uuid(opener)
    assert uuid == control_uuid


@pytest.mark.skipif("TRAVIS" in os.environ and os.environ["TRAVIS"] == "true",
                    reason="Skipping this test on Travis CI."
                    )
def test_esa_scihub_download(s1_grd_notnr_ost_product, mirror=1):
    herbert_uname = HERBERT_USER['uname']
    herbert_password = HERBERT_USER['pword']
    df = pd.DataFrame({'identifier': [s1_grd_notnr_ost_product[1].scene_id]})
    with TemporaryDirectory(dir=os.getcwd()) as temp:
        download_sentinel1(
            inventory_df=df,
            download_dir=temp,
            mirror=mirror,
            concurrent=1,
            uname=herbert_uname,
            pword=herbert_password
        )

        product_path = s1_grd_notnr_ost_product[1].get_path(
            download_dir=temp,
            data_mount='/eodata'
        )
        return_code = check_zipfile(product_path)
        assert return_code is None


@pytest.mark.skip(
    reason="ASF download is tested in the test_project batch whatever!"
)
def test_asf_download(s1_grd_notnr_ost_product, mirror=2):
    herbert_uname = HERBERT_USER['uname']
    herbert_password = HERBERT_USER['asf_pword']
    df = pd.DataFrame({'identifier': [s1_grd_notnr_ost_product[1].scene_id]})
    with TemporaryDirectory(dir=os.getcwd()) as temp:
        download_sentinel1(
            inventory_df=df,
            download_dir=temp,
            mirror=mirror,
            concurrent=1,
            uname=herbert_uname,
            pword=herbert_password
        )
        from ost.helpers.helpers import check_zipfile
        product_path = s1_grd_notnr_ost_product[1].get_path(
            download_dir=temp,
            data_mount='/eodata'
        )
        return_code = check_zipfile(product_path)
        assert return_code is None
