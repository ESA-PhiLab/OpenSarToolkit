from ost.helpers.settings import config_check


def test_update_ard_param(slc_project_class):

    slc_project_class.update_ard_parameters(ard_type='OST-GTC')
    assert slc_project_class.config_dict['processing']['single_ARD']['ard_type'] == 'OST-GTC'


# Test GRDs to ARD kind of batch
def test_grds_to_ards(grd_project_class):
    for ard_type in config_check['ard_type']['choices']:
        grd_project_class.update_ard_parameters(ard_type)
        grd_project_class.grds_to_ards(
            inventory_df=grd_project_class.refined_inventory_dict['DESCENDING_VVVH'],
            timeseries=False,
            timescan=False,
            mosaic=False,
            overwrite=True
        )

# TODO: Runs endlessly on travis for whatever reason
# # Test burst batch for all slc ARD types,
# split into more so that TRAVIS, wont get crazy
# # [OST-GTC, OST-RTC, OST-minimal]

# def test_burst_batch_ost_gtc(slc_project_class):
#     slc_project_class.ard_parameters["single_ARD"]["type"] = 'OST-GTC'
#     slc_project_class.update_ard_parameters()
#     slc_project_class.ard_parameters['single_ARD']['resolution'] = 50
#     slc_project_class.bursts_to_ard(
#         timeseries=False,
#         timescan=False,
#         mosaic=False,
#         overwrite=True,
#         cut_to_aoi=False,
#         ncores=2
#     )
# def test_burst_batch_ost_rtc(slc_project_class):
#     slc_project_class.ard_parameters["single_ARD"]["type"] = 'OST-RTC'
#     slc_project_class.update_ard_parameters()
#     slc_project_class.ard_parameters['single_ARD']['resolution'] = 50
#     slc_project_class.bursts_to_ard(
#         timeseries=False,
#         timescan=False,
#         mosaic=False,
#         overwrite=True,
#         cut_to_aoi=False,
#         ncores=2
#     )
