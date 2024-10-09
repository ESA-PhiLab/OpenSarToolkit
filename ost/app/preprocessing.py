import sys
import os
from pathlib import Path
from pprint import pprint
from ost import Sentinel1Scene
import click

@click.command()
@click.argument("input")
@click.option("--resolution",
              default=100)
@click.option("--ard-type",
              type=click.Choice(['OST_GTC', 'OST-RTC', 'CEOS', 'Earth-Engine']),
              default='Earth-Engine')
@click.option("--with-speckle-filter",
              default=False)
@click.option("--resampling-method",
              type=click.Choice(["BILINEAR_INTERPOLATION", "BICUBIC_INTERPOLATION"]),
              default="BILINEAR_INTERPOLATION")
@click.option("--cdse-user", default="dummy")
@click.option("--cdse-password", default="dummy")

def run(
    input: str,
    resolution: int,
    ard_type: str,
    with_speckle_filter: bool,
    resampling_method: str,
    cdse_user: str,
    cdse_password: str,
):
    # get home folder
    #home = Path.home()
    home = Path(".")

    # create a processing directory
    #output_dir = home.joinpath('OST_Tutorials', 'Tutorial_1')
    #output_dir.mkdir(parents=True, exist_ok=True)
    #print(str(output_dir))
    output_dir = "/home/ost/shared"
    output_path = Path(output_dir)

    # create a S1Scene class instance based on the scene identifier of the first ever Dual-Pol Sentinel-1 IW product

    #---------------------------------------------------
    # Some scenes to choose from

    # very first IW (VV/VH) S1 image available over Istanbul/Turkey
    # NOTE:only available via ASF data mirror
    #scene_id = 'S1A_IW_GRDH_1SDV_20141003T040550_20141003T040619_002660_002F64_EC04'
    #scene_id = 'S1A_IW_GRDH_1SDV_20221004T164316_20221004T164341_045295_056A44_13CB'
    scene_id = input[input.rfind("/")+1:input.rfind(".")]
    year = scene_id[17:21]
    month = scene_id[21:23]
    day = scene_id[23:25]
    os.makedirs(f"{output_dir}/SAR/GRD/{year}/{month}/{day}", exist_ok=True)
    try:
        os.link(input, f"{output_dir}/SAR/GRD/{year}/{month}/{day}/{scene_id}.zip")
        with open(f"{output_dir}/SAR/GRD/{year}/{month}/{day}/{scene_id}.downloaded", mode="w") as f:
            f.write("successfully found here")
    except:
        pass

    # other scenes with different scene types to process (uncomment)
    # IW scene (dual-polarised HH/HV) over Norway/Spitzbergen
    # scene_id = 'S1B_IW_GRDH_1SDH_20200325T150411_20200325T150436_020850_02789D_2B85'

    # IW scene (single-polarised VV) over Ecuadorian Amazon
    # scene_id = 'S1A_IW_GRDH_1SSV_20150205T232009_20150205T232034_004494_00583A_1C80'

    # EW scene (dual-polarised VV/VH) over Azores (needs a different DEM, see cell of ARD parameters below)
    # scene_id = 'S1B_EW_GRDM_1SDV_20200303T193150_20200303T193250_020532_026E82_5CE9'

    # EW scene (dual-polarised HH/HV) over Greenland
    # scene_id = 'S1B_EW_GRDM_1SDH_20200511T205319_20200511T205419_021539_028E4E_697E'

    # Stripmap mode S5 scene (dual-polarised VV/VH) over Germany
    # scene_id = 'S1B_S5_GRDH_1SDV_20170104T052519_20170104T052548_003694_006587_86AB'
    #---------------------------------------------------

    # create an S1Scene instance
    s1 = Sentinel1Scene(scene_id)

    # print summarising infos about the scene
    s1.info()

    s1.download(output_path, mirror="5", uname=cdse_user, pword=cdse_password)

    # Template ARD parameters

    # we change ARD type
    # possible choices are:
    # 'OST_GTC', 'OST-RTC', 'CEOS', 'Earth Engine'
    #s1.update_ard_parameters('Earth-Engine')
    s1.update_ard_parameters(ard_type)
    print('-----------------------------------------------------------------------------------------------------------')
    print('Dictionary of Earth Engine ARD parameters:')
    print('-----------------------------------------------------------------------------------------------------------')
    pprint(s1.ard_parameters['single_ARD'])
    print('-----------------------------------------------------------------------------------------------------------')

    # Customised ARD parameters

    # we cusomize the resolution and image resampling
    s1.ard_parameters['single_ARD']['resolution'] = resolution  # set output resolution to 100m
    s1.ard_parameters['single_ARD']['remove_speckle'] = with_speckle_filter # apply a speckle filter
    s1.ard_parameters['single_ARD']['dem']['image_resampling'] = resampling_method # BICUBIC_INTERPOLATION is default
    s1.ard_parameters['single_ARD']['to_tif'] = True

    # s1.ard_parameters['single_ARD']['product_type'] = 'RTC-gamma0'

    # uncomment this for the Azores EW scene
    # s1.ard_parameters['single_ARD']['dem']['dem_name'] = 'GETASSE30'
    print('-----------------------------------------------------------------------------------------------------------')
    print('Dictionary of our customised ARD parameters for the final scene processing:')
    print('-----------------------------------------------------------------------------------------------------------')
    pprint(s1.ard_parameters['single_ARD'])
    print('-----------------------------------------------------------------------------------------------------------')

    s1.create_ard(
        infile=s1.get_path(output_path),
        out_dir=output_path,
        overwrite=True
    )

    print(' The path to our newly created ARD product can be obtained the following way:')
    print(f"CALVALUS_OUTPUT_PRODUCT {s1.ard_dimap}")

#    s1.create_rgb(outfile = output_path.joinpath(f'{s1.start_date}.tif'))

#    print(' The path to our newly created RGB product can be obtained the following way:')
#    print(f"CALVALUS_OUTPUT_PRODUCT {s1.ard_rgb}")

#from ost.helpers.settings import set_log_level
#import logging
#set_log_level(logging.DEBUG)

import click

if __name__ == "__main__":
    sys.exit(run())

