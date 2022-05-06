import pytest

from ost import Sentinel1Scene


@pytest.mark.skip(reason="not running in pip build")
def test_s1scene_metadata(s1_id):
    s1 = Sentinel1Scene(s1_id)
    control_id = "S1A_IW_GRDH_1SDV_20141003T040550_" "20141003T040619_002660_002F64_EC04"
    control_dict = {
        "Scene_Identifier": "S1A_IW_GRDH_1SDV_20141003T040550_20141003T040619_" "002660_002F64_EC04",
        "Satellite": "Sentinel-1A",
        "Acquisition_Mode": "Interferometric Wide Swath",
        "Processing_Level": "1",
        "Product_Type": "Ground Range Detected (GRD)",
        "Acquisition_Date": "20141003",
        "Start_Time": "040550",
        "Stop_Time": "040619",
        "Absolute_Orbit": "002660",
        "Relative_Orbit": "138",
    }
    assert control_dict == s1.info_dict()
    assert s1.scene_id == control_id
