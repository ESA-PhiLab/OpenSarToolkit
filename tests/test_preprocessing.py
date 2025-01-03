import os
from pathlib import Path

# OST insists on knowing the path to gpt, but we don't need it for these tests.
os.environ["GPT_PATH"] = os.environ.get("GPT_PATH", "dummy")

from ost.app import preprocessing


def test_get_input_path_from_stac_zip():
    cat_path = Path(__file__).parent / "resources" / "input_zip"
    assert preprocessing.get_input_path_from_stac(str(cat_path)) == \
        str(cat_path /
            "S1A_IW_GRDH_1SDV_20221004T164316_20221004T164341_"
            "045295_056A44_13CB.zip")


def test_get_input_path_from_stac_dir():
    cat_path = Path(__file__).parent / "resources" / "input_dir"
    assert preprocessing.get_input_path_from_stac(str(cat_path)) == \
           str(cat_path /
               "S1A_IW_GRDH_1SDV_20241113T170607_20241113T170632_"
               "056539_06EEA8_B145.SAFE")
