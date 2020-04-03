from ost.helpers.settings import config_check


def test_update_ard_param(slc_project_class):
    slc_project_class.ard_parameters["single_ARD"]["type"] = 'OST-GTC'
    slc_project_class.update_ard_parameters()
    assert slc_project_class.project_dict["processing"]["single_ARD"]["type"] == 'OST-GTC'


# Test burst batch for all slc ARD types
# [OST-GTC, OST-RTC, OST-minimal]
def test_burst_batch_ost_gtc(slc_project_class):
    slc_project_class.ard_parameters["single_ARD"]["type"] = 'OST-GTC'
    slc_project_class.update_ard_parameters()
    slc_project_class.bursts_to_ard(
        timeseries=False,
        timescan=False,
        mosaic=False,
        overwrite=False,
        cut_to_aoi=False,
    )


def test_burst_batch_ost_rtc(slc_project_class):
    slc_project_class.bursts_to_ard(
        timeseries=False,
        timescan=False,
        mosaic=False,
        overwrite=False,
        cut_to_aoi=False,
    )


# Test GRDs to ARD kind of batch
def test_grds_to_ard(grd_project_class):
    for ard_type in config_check['type']['choices']:
        grd_project_class.ard_parameters["single_ARD"]["type"] = ard_type
        grd_project_class.update_ard_parameters()
        grd_project_class.grds_to_ard(
            timeseries=False,
            timescan=False,
            mosaic=False,
            overwrite=False,
            cut_to_aoi=False
        )
