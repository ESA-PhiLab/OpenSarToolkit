from datetime import datetime
import sys
import os
import pathlib
from pathlib import Path
import pprint
import logging
import shutil

from ost import Sentinel1Scene
import click
import pystac
import rasterio


LOGGER = logging.getLogger(__name__)
CATALOG_FILENAME = "catalog.json"
ITEM_ID = "result-item"


@click.command()
@click.argument("input_", metavar="input")
@click.option("--resolution", default=100)
@click.option(
    "--ard-type",
    type=click.Choice(["OST_GTC", "OST-RTC", "CEOS", "Earth-Engine"]),
    default="Earth-Engine",
)
@click.option("--with-speckle-filter", is_flag=True, default=False)
@click.option(
    "--resampling-method",
    type=click.Choice(["BILINEAR_INTERPOLATION", "BICUBIC_INTERPOLATION"]),
    default="BILINEAR_INTERPOLATION",
)
@click.option("--cdse-user", default="dummy")
@click.option("--cdse-password", default="dummy")
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Skip processing and write a placeholder output file instead. "
    "Useful for testing.",
)
@click.option(
    "--wipe-cwd",
    is_flag=True,
    default=False,
    help="After processing, delete everything in the current working directory "
    "except for the output data and STAC entries. Dangerous, but can be useful "
    "when executing as an application package.",
)
def run(
    input_: str,
    resolution: int,
    ard_type: str,
    with_speckle_filter: bool,
    resampling_method: str,
    cdse_user: str,
    cdse_password: str,
    dry_run: bool,
    wipe_cwd: bool,
):
    horizontal_line = "-" * 79  # Used in log output

    logging.basicConfig(level=logging.INFO)
    # from ost.helpers.settings import set_log_level
    # import logging
    # set_log_level(logging.DEBUG)

    scene_presets = {
        # very first IW (VV/VH) S1 image available over Istanbul/Turkey
        # NOTE:only available via ASF data mirror
        "istanbul": "S1A_IW_GRDH_1SDV_20141003T040550_20141003T040619_002660_002F64_EC04",
        # ???
        "unknown": "S1A_IW_GRDH_1SDV_20221004T164316_20221004T164341_045295_056A44_13CB",
        # IW scene (dual-polarised HH/HV) over Norway/Spitzbergen
        "spitzbergen": "S1B_IW_GRDH_1SDH_20200325T150411_20200325T150436_020850_02789D_2B85",
        # IW scene (single-polarised VV) over Ecuadorian Amazon
        "ecuador": "S1A_IW_GRDH_1SSV_20150205T232009_20150205T232034_004494_00583A_1C80",
        # EW scene (dual-polarised VV/VH) over Azores
        # (needs a different DEM,see ARD parameters below)
        "azores": "S1B_EW_GRDM_1SDV_20200303T193150_20200303T193250_020532_026E82_5CE9",
        # EW scene (dual-polarised HH/HV) over Greenland
        "greenland": "S1B_EW_GRDM_1SDH_20200511T205319_20200511T205419_021539_028E4E_697E",
        # Stripmap mode S5 scene (dual-polarised VV/VH) over Germany
        "germany": "S1B_S5_GRDH_1SDV_20170104T052519_20170104T052548_003694_006587_86AB",
    }

    # "When executed, the Application working directory is also the Application
    # output directory. Any file created by the Application should be added
    # under that directory." -- https://docs.ogc.org/bp/20-089r1.html#toc20
    output_dir = os.getcwd()
    output_path = Path(output_dir)

    # We expect input to be the path to a directory containing a STAC catalog
    # containing an item which contains an asset for either a zip file
    # (zipped SAFE archive) or a SAFE manifest (which is used to determine
    # the location of a non-zipped SAFE directory). The returned path is
    # either the zip file or the SAFE directory.
    input_path = get_input_path_from_stac(input_)

    # We assume that any file input path is a zip, and any non-file input
    # path is a SAFE directory.
    zip_input = pathlib.Path(input_path).is_file()
    LOGGER.info(f"Input is {'zip' if zip_input else 'SAFE directory'}")

    scene_id = input_path[input_path.rfind("/") + 1 : input_path.rfind(".")]
    if zip_input:
        copy_zip_input(input_path, output_dir, scene_id)

    # Instantiate a Sentinel1Scene from the specified scene identifier
    s1 = Sentinel1Scene(scene_id)
    s1.info()  # write scene summary information to stdout
    if zip_input:
        s1.download(
            output_path, mirror="5", uname=cdse_user, pword=cdse_password
        )

    single_ard = s1.ard_parameters["single_ARD"]
    # Set ARD type. Choices: "OST_GTC", "OST-RTC", "CEOS", "Earth Engine"
    s1.update_ard_parameters(ard_type)
    LOGGER.info(
        f"{horizontal_line}\n"
        f"Dictionary of Earth Engine ARD parameters:\n"
        f"{horizontal_line}\n"
        f"{pprint.pformat(single_ard)}\n"
        f"{horizontal_line}"
    )

    # Customize ARD parameters
    single_ard["resolution"] = resolution
    single_ard["remove_speckle"] = with_speckle_filter
    single_ard["dem"][
        "image_resampling"
    ] = resampling_method  # default: BICUBIC_INTERPOLATION
    single_ard["to_tif"] = True
    # single_ard['product_type'] = 'RTC-gamma0'

    # uncomment this for the Azores EW scene
    # s1.ard_parameters['single_ARD']['dem']['dem_name'] = 'GETASSE30'

    LOGGER.info(
        f"{horizontal_line}\n"
        "Dictionary of customized ARD parameters for final scene processing:\n"
        f"{horizontal_line}\n"
        f"{pprint.pformat(single_ard)}\n"
        f"{horizontal_line}"
    )

    tiff_dir = output_path / ITEM_ID
    tiff_dir.mkdir(exist_ok=True)
    tiff_path = tiff_dir / f"{s1.start_date}.tif"
    if dry_run:
        LOGGER.info(f"Dry run -- creating dummy output at {tiff_path}")
        create_dummy_tiff(tiff_path)
    else:
        LOGGER.info(f"Creating ARD at {output_path}")
        # create_ard seems to be a prerequisite for create_rgb.
        if zip_input:
            s1.create_ard(
                infile=s1.get_path(output_path),
                out_dir=output_path,
                overwrite=True,
            )
        else:
            s1.create_ard(
                infile=input_path, out_dir=output_path, overwrite=True
            )

        LOGGER.info(f"Path to newly created ARD product: {s1.ard_dimap}")
        LOGGER.info(f"Creating RGB at {output_path}")
        s1.create_rgb(outfile=tiff_path)
        LOGGER.info(f"Path to newly created RGB product: {tiff_path}")

    # Write a STAC catalog and item pointing to the output product.
    LOGGER.info("Writing STAC catalogue and item")
    write_stac_for_tiff(str(output_path), str(tiff_path), scene_id, resolution)
    if wipe_cwd:
        LOGGER.info("Removing everything except output from CWD")
        delete_cwd_contents()

def copy_zip_input(input_path, output_dir, scene_id):
    year = scene_id[17:21]
    month = scene_id[21:23]
    day = scene_id[23:25]
    output_subdir = f"{output_dir}/SAR/GRD/{year}/{month}/{day}"
    os.makedirs(output_subdir, exist_ok=True)
    try:
        scene_path = f"{output_subdir}/{scene_id}"
        try:
            os.link(input_path, f"{scene_path}.zip")
        except OSError as e:
            LOGGER.warning("Exception linking input data", exc_info=e)
            LOGGER.warning("Attempting to copy instead.")
            shutil.copy2(input_path, f"{scene_path}.zip")
        with open(f"{scene_path}.downloaded", mode="w") as f:
            f.write("successfully found here")
    except Exception as e:
        LOGGER.warning("Exception linking/copying input data", exc_info=e)


def create_dummy_tiff(path: Path) -> None:
    import numpy as np
    import rasterio

    data = np.fromfunction(
        lambda x, y: x / 2000 + np.sin(y / 50), (2000, 2000)
    )
    with rasterio.open(
        str(path),
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=data.dtype,
        crs="+proj=latlong",
        transform=rasterio.transform.Affine.scale(0.1, 0.1),
        tiled=True,
    ) as dst:
        dst.write(data, 1)


def get_input_path_from_stac(stac_root: str) -> str:
    stac_path = pathlib.Path(stac_root)
    catalog = pystac.Catalog.from_file(str(stac_path / "catalog.json"))
    item_links = [link for link in catalog.links if link.rel == "item"]
    assert len(item_links) == 1
    item_link = item_links[0]
    item_path = stac_path / item_link.href
    item = pystac.Item.from_file(str(item_path))
    if "manifest" in item.assets:
        LOGGER.info(f"Found manifest asset in {str(item_path)}")
        manifest_asset = item.assets["manifest"]
        if "filename" in manifest_asset.extra_fields:
            filename = pathlib.Path(manifest_asset.extra_fields["filename"])
            LOGGER.info(f"Asset path in item: {str(filename)}")
            safe_dir = item_path / filename.parent
            LOGGER.info(f"Resolved SAFE directory path to {safe_dir}")
            assert safe_dir.exists(), "SAFE directory does not exist"
            assert safe_dir.is_dir(), "SAFE directory is not a directory"
            return str(safe_dir)
        else:
            raise RuntimeError(f"No filename for manifest asset in {catalog}")
    else:
        LOGGER.info("No manifest asset found; looking for zip asset")
        zip_assets = [
            asset
            for asset in item.assets.values()
            if asset.media_type == "application/zip"
        ]
        if len(zip_assets) < 1:
            raise RuntimeError(
                f"No manifest assets or zip assets found in {catalog}"
            )
        elif len(zip_assets) > 1:
            raise RuntimeError(
                f"No manifest assets and multiple zip assets found in "
                f"{stac_root}, so it's not clear which zip asset to use."
            )
        else:
            zip_path = stac_path / zip_assets[0].href
            LOGGER.info(f"Found input zip at {zip_path}")
            return str(zip_path)


def write_stac_for_tiff(
    stac_root: str, asset_path: str, scene_id: str, gsd: int
) -> None:
    LOGGER.info(f"Writing STAC for asset {asset_path} to {stac_root}")
    ds = rasterio.open(asset_path)
    asset = pystac.Asset(
        roles=["data", "visual"],
        href=asset_path,
        media_type="image/tiff; application=geotiff;",
        title="OST-processed",
        extra_fields=dict(gsd=gsd),
    )
    bb = ds.bounds
    s = scene_id
    item = pystac.Item(
        id=ITEM_ID,
        geometry={
            "type": "Polygon",
            "coordinates": [
                [bb.left, bb.bottom],
                [bb.left, bb.top],
                [bb.right, bb.top],
                [bb.right, bb.bottom],
                [bb.left, bb.bottom],
            ],
        },
        bbox=[bb.left, bb.bottom, bb.right, bb.top],
        # Datetime is required by the STAC specification and schema, even
        # when there is no reasonable value for it to take. In such cases
        # it is permitted to set datetime to null, but not to omit it.
        datetime=None,
        start_datetime=datetime(
            *map(
                int,
                (s[17:21], s[21:23], s[23:25], s[26:28], s[28:30], s[30:32]),
            )
        ),
        end_datetime=datetime(
            *map(
                int,
                (s[33:37], s[37:39], s[39:41], s[42:44], s[44:46], s[46:48]),
            )
        ),
        properties={},  # datetime values will be filled in automatically
        assets={"TIFF": asset},
    )
    catalog = pystac.Catalog(
        id="catalog",
        description="Root catalog",
        href=f"{stac_root}/{CATALOG_FILENAME}",
    )
    catalog.add_item(item)
    catalog.make_all_asset_hrefs_relative()
    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)


def delete_cwd_contents():
    """Delete everything except the output data and STAC files"""

    cwd = Path.cwd()
    for member in cwd.iterdir():
        if member.name not in {CATALOG_FILENAME, ITEM_ID}:
            if member.is_dir():
                shutil.rmtree(member)
            if member.is_file():
                member.unlink()


if __name__ == "__main__":
    sys.exit(run())
