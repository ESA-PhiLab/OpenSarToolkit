ost.s1.s1scene.Sentinel1Scene
==============================

.. autoclass:: ost.s1.s1scene.Sentinel1Scene

    .. rubric:: Attributes

    .. autosummary::

        ~Sentinel1Scene.scene_id = scene_id
        ~Sentinel1Scene.mission_id = scene_id[0:3]
        ~Sentinel1Scene.mode_beam = scene_id[4:6]
        ~Sentinel1Scene.product_type = scene_id[7:10]
        ~Sentinel1Scene.resolution_class = scene_id[10]
        ~Sentinel1Scene.proc_level = scene_id[12]
        ~Sentinel1Scene.pol_mode = scene_id[14:16]
        ~Sentinel1Scene.start_date = scene_id[17:25]
        ~Sentinel1Scene.start_time = scene_id[26:32]
        ~Sentinel1Scene.stop_date = scene_id[33:41]
        ~Sentinel1Scene.stop_time = scene_id[42:48]
        ~Sentinel1Scene.abs_orbit = scene_id[49:55]
        ~Sentinel1Scene.data_take_id = scene_id[57:62]
        ~Sentinel1Scene.unique_id = scene_id[63:]
        ~Sentinel1Scene.year = scene_id[17:21]
        ~Sentinel1Scene.month = scene_id[21:23]
        ~Sentinel1Scene.day = scene_id[23:25]
        ~Sentinel1Scene.onda_class = scene_id[4:14]
        ~Sentinel1Scene.orbit_offset
        ~Sentinel1Scene.satellite
        ~Sentinel1Scene.rel_orbit
        ~Sentinel1Scene.acq_mode
        ~Sentinel1Scene.p_type
        ~Sentinel1Scene.product_dl_path
        ~Sentinel1Scene.ard_dimap
        ~Sentinel1Scene.ard_rgb
        ~Sentinel1Scene.rgb_thumbnail
        ~Sentinel1Scene.config_dict
        ~Sentinel1Scene.config_file

    .. rubric:: Methods

    .. autosummary::
        :nosignatures:

        ~Sentinel1Scene.info
        ~Sentinel1Scene.info_dict
        ~Sentinel1Scene.download
        ~Sentinel1Scene.download_path
        ~Sentinel1Scene.get_path
        ~Sentinel1Scene.scihub_uuid
        ~Sentinel1Scene.scihub_url
        ~Sentinel1Scene.scihub_md5
        ~Sentinel1Scene.scihub_online_status
        ~Sentinel1Scene.scihub_trigger_production
        ~Sentinel1Scene.scihub_annotation_get
        ~Sentinel1Scene.zip_annotation_get
        ~Sentinel1Scene.safe_annotation_get
        ~Sentinel1Scene.ondadias_uuid
        ~Sentinel1Scene.asf_url
        ~Sentinel1Scene.peps_uuid
        ~Sentinel1Scene.peps_online_status
        ~Sentinel1Scene.get_ard_parameters
        ~Sentinel1Scene.update_ard_parameters
        ~Sentinel1Scene.set_external_dem
        ~Sentinel1Scene.create_ard
        ~Sentinel1Scene.create_rgb
        ~Sentinel1Scene.create_rgb_thumbnail
        ~Sentinel1Scene.visualise_rgb

.. automethod:: ost.s1.s1scene.Sentinel1Scene.info

.. automethod:: ost.s1.s1scene.Sentinel1Scene.info_dict

.. automethod:: ost.s1.s1scene.Sentinel1Scene.download

.. automethod:: ost.s1.s1scene.Sentinel1Scene.download_path

.. automethod:: ost.s1.s1scene.Sentinel1Scene.get_path

.. automethod:: ost.s1.s1scene.Sentinel1Scene.scihub_uuid

.. automethod:: ost.s1.s1scene.Sentinel1Scene.scihub_url

.. automethod:: ost.s1.s1scene.Sentinel1Scene.scihub_md5

.. automethod:: ost.s1.s1scene.Sentinel1Scene.scihub_online_status

.. automethod:: ost.s1.s1scene.Sentinel1Scene.scihub_trigger_production

.. automethod:: ost.s1.s1scene.Sentinel1Scene.scihub_annotation_get

.. automethod:: ost.s1.s1scene.Sentinel1Scene.zip_annotation_get

.. automethod:: ost.s1.s1scene.Sentinel1Scene.safe_annotation_get

.. automethod:: ost.s1.s1scene.Sentinel1Scene.ondadias_uuid

.. automethod:: ost.s1.s1scene.Sentinel1Scene.asf_url

.. automethod:: ost.s1.s1scene.Sentinel1Scene.peps_uuid

.. automethod:: ost.s1.s1scene.Sentinel1Scene.peps_online_status

.. automethod:: ost.s1.s1scene.Sentinel1Scene.get_ard_parameters

.. automethod:: ost.s1.s1scene.Sentinel1Scene.update_ard_parameters

.. automethod:: ost.s1.s1scene.Sentinel1Scene.set_external_dem

.. automethod:: ost.s1.s1scene.Sentinel1Scene.create_ard

.. automethod:: ost.s1.s1scene.Sentinel1Scene.create_rgb

.. automethod:: ost.s1.s1scene.Sentinel1Scene.create_rgb_thumbnail

.. automethod:: ost.s1.s1scene.Sentinel1Scene.visualise_rgb
