from ost import Sentinel1_Scene


def test_s1scene_metadata(s1_id):
    s1 = Sentinel1_Scene(s1_id)
    s1_info = s1.info()
    control = {
        'Scene_Identifier':
            'S1A_IW_GRDH_1SDV_20191116T170638_20191116T170703_029939_036AAB_070F',
        'Satellite': 'Sentinel-1A',
        'Acquisition_Mode': 'Interferometric Wide Swath',
        'Processing_Level': '1',
        'Product_Type': 'Ground Range Detected (GRD)',
        'Acquisition_Date': '20191116',
        'Start_Time': '170638',
        'Stop_Time': '170703',
        'Absolute_Orbit': '029939',
        'Relative_Orbit': '117'
    }
    assert s1_info == control
