# -*- coding: utf-8 -*-
import unittest

from ost import S1Scene


class TestS1Scene(unittest.TestCase):
    
    scene_id = ['S1A_IW_GRDH_1SDV_20191104T170638_20191104T170703_029764_036486_DB88']
    s1 = S1Scene(self.scene_id)
    
    def test_attributes(self):
        
        self.assertIs(self.s1.scene_id, self.scene_id)
        self.assertIs(self.s1.mission_id, 'S1A')
        

    def test_paths(self):
        
        
    
if __name__ == '__main__':
    unittest.main()