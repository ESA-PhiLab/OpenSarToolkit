from ost.s1.s1scene import Sentinel1_Scene as S1Scene


def test_s1_scene_metadata(s1_id):
    scene = S1Scene(scene_id=s1_id)
    assert scene.scene_id == s1_id
