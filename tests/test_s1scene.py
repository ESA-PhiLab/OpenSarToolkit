from ost import Sentinel1_Scene


def test_s1scene_metadata(s1_id):
    s1 = Sentinel1_Scene(s1_id)
    s1.info()
    control_id = 'S1A_IW_GRDH_1SDV_20141003T040550_20141003T040619_002660_002F64_EC04'
    assert s1.scene_id == control_id
