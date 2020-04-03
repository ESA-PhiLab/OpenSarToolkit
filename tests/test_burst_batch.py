

# Test burst batch for all slc ARD types
# [OST-GTC, OST-RTC, OST-minimal]
def test_burst_batch(slc_project_class):
    slc_project_class.bursts_to_ard(
        timeseries=False,
        timescan=False,
        mosaic=False,
        overwrite=False,
        cut_to_aoi=False
    )
