from ost import Sentinel1_Scene


def test_s1scene_metadata(s1_id):
    s1 = Sentinel1_Scene(s1_id)
    s1.info()
    control_id = 'S1A_IW_GRDH_1SDV_20191116T170638_20191116T170703_029939_036AAB_070F'
    assert s1.scene_id == control_id
