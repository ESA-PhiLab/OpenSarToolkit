"""This module provides helper functions for Copernicus Dataspace API."""

import getpass
import logging
import multiprocessing
import urllib
from pathlib import Path
from datetime import datetime as dt
import tqdm
import requests
from shapely.wkt import loads
from ost.helpers import helpers as h

logger = logging.getLogger(__name__)

def ask_credentials():
    """Interactive function to ask for Copernicus credentials."""
    print(
        "If you do not have a Copernicus dataspace user account"
        " go to: https://dataspace.copernicus.eu/ and register"
    )
    uname = input("Your Copernicus Dataspace Username:")
    pword = getpass.getpass("Your Copernicus Dataspace Password:")

    return uname, pword

def connect(uname=None, pword=None, base_url="https://catalogue.dataspace.copernicus.eu"):
    """Generates an opener for the Copernicus apihub/dhus

    :param uname: username of Copernicus' CDSE
    :type uname: str
    :param pword: password of Copernicus' CDSE
    :type pword: str
    :param base_url:
    :return: an urllib opener instance for Copernicus' CDSE
    :rtype: opener object
    """

    if not uname:
        print(" If you do not have a CDSE user" " account go to: https://browser.dataspace.copernicus.eu")
        uname = input(" Your CDSE Username:")

    if not pword:
        pword = getpass.getpass(" Your CDSE Password:")

    # create opener
    manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    manager.add_password(None, base_url, uname, pword)
    handler = urllib.request.HTTPBasicAuthHandler(manager)
    opener = urllib.request.build_opener(handler)

    return opener

def get_access_token(username, password: None):

    if not password:
        logger.info(' Please provide your Copernicus Dataspace password:')
        password = getpass.getpass()

    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
        }
    try:
        r = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data,
        )
        r.raise_for_status()
    except Exception as e:
        raise Exception(
            f"Access token creation failed. Reponse from the server was: {r.json()}"
            )
    return r.json()["access_token"]


def refresh_access_token(refresh_token: str) -> str:
    data = {
        "client_id": "cdse-public",
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }

    try:
        r = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data,
        )
        r.raise_for_status()
    except Exception as e:
        raise Exception(
            f"Access token refresh failed. Reponse from the server was: {r.json()}"
        )

    return r.json()["access_token"]


def create_aoi_str(aoi):
    """Convert WKT formatted AOI to dataspace's geometry attribute."""
    # load to shapely geometry to easily test for geometry type
    geom = loads(aoi)

    # dependent on the type construct the query string
    if geom.geom_type == "Point":
        return f'&lon={geom.y}&lat={geom.x}'

    else:
        # simplify geometry, as we might otherwise bump into too long string issue
        aoi_convex = geom.convex_hull

        # create scihub-confrom aoi string
        return f'&geometry={aoi_convex}'

def create_toi_str(start="2014-10-01", end=dt.now().strftime("%Y-%m-%d")):
    """Convert start and end date to scihub's search url time period attribute."""
    # bring start and end date to query format
    return f"&startDate={start}T00:00:00Z&completionDate={end}T23:59:59Z"

def create_s1_product_specs(product_type=None, polarisation=None, beam=None):
    """Convert Sentinel-1's product metadata to scihub's product attributes."""
    # transform product type, polarisation and beam to query format
    product_type_query = f'&productType={product_type}' if product_type else ''
    polarisation_query = f'&polarisation={polarisation.replace(" ", "%26")}' if polarisation else ''
    sensor_mode_query = f'&sensorMode={beam}' if beam else ''

    return product_type_query + polarisation_query + sensor_mode_query


def extract_basic_metadata(properties):

    # those are the things we wnat out of the standard json
    wanted = ['title', 'orbitDirection', 'platform', 'polarisation', 'swath', 'thumbnail', 'published']

    # loop through all properties
    _dict = {}
    for k, v in properties.items():
        # consider if in the list of wanted properties
        if k in wanted:
            if k == 'polarisation':
                # remove & sign
                _dict[k] = v.replace('&', ' ')
            elif k == 'title':
                #  remove .SAFE extension
                _dict[k] = v[:-5]
            elif k == 'thumbnail':
                _dict[k] = '/'.join(v.split('/')[:-2]) + '/manifest.safe'
            else:
                _dict[k] = v

    sorted_dict = dict(sorted(_dict.items(), key=lambda item: wanted.index(item[0])))
    return sorted_dict.values()


def get_entry(line):

    return line.split('>')[1].split('<')[0]


def get_advanced_metadata(metafile, access_token):

    with requests.Session() as session:
        headers={'Authorization': f'Bearer {access_token}'}
        request = session.request("get", metafile)
        response = session.get(request.url, headers=headers, stream=True)

    for line in response.iter_lines():

        line = line.decode('utf-8')
        if 's1sarl1:sliceNumber' in line:
            slicenumber = get_entry(line)
        if 's1sarl1:totalSlices' in line:
            total_slices =  get_entry(line)
        if 'relativeOrbitNumber type="start"' in line:
            relativeorbit = get_entry(line)
        if 'relativeOrbitNumber type="stop"' in line:
            lastrelativeorbit =  get_entry(line)
        if 'safe:nssdcIdentifier' in line:
            platformidentifier = get_entry(line)
        if 's1sarl1:missionDataTakeID' in line:
            missiondatatakeid = get_entry(line)
        if 's1sarl1:mode' in line:
            sensoroperationalmode = get_entry(line)
        if 'orbitNumber type="start"' in line:
            orbitnumber = get_entry(line)
        if 'orbitNumber type="stop"' in line:
            lastorbitnumber = get_entry(line)
        if 'safe:startTime' in line:
            beginposition = get_entry(line)
        if 'safe:stopTime' in line:
            endposition = get_entry(line)
        if '1sarl1:productType' in line:
            product_type = get_entry(line)

    # add acquisitiondate
    acqdate = dt.strftime(dt.strptime(beginposition, '%Y-%m-%dT%H:%M:%S.%f'), format='%Y%m%d')

    return (
        slicenumber, total_slices,
        relativeorbit, lastrelativeorbit,
        platformidentifier, missiondatatakeid,
        sensoroperationalmode, product_type,
        orbitnumber, lastorbitnumber,
        beginposition, endposition, acqdate,
        0 # placeholder for size
    )


def s1_download(uuid, filename, uname, pword, base_url="https://catalogue.dataspace.copernicus.eu"):
    """Single scene download function for CDSE

    :param uuid: product's uuid
    :param filename: local path for the download
    :param uname: username of CDSE
    :param pword: password of CDSE
    :param base_url:

    :return:
    """

    # get out the arguments
    if isinstance(filename, str):
        filename = Path(filename)

    # check if file is partially downloaded
    first_byte = filename.stat().st_size if filename.exists() else 0

    # ask for credentials in case they are not defined as input
    if not uname or not pword:
        ask_credentials()

    # define url
    url = f"{base_url}/odata/v1/Products({uuid})/$value"

    # get first response for file Size
    access_token = get_access_token(uname, pword)
    # we use some random url for checking (also for czech mirror)
    with requests.Session() as session:
        headers = {'Authorization': f'Bearer {access_token}',
                   "Range": f"bytes={first_byte}-"}
        request = session.request("get", url)
        response = session.get(request.url, headers=headers, stream=True)

        # check response
        if response.status_code == 401:
            raise ValueError(" ERROR: Username/Password are incorrect.")
        elif response.status_code != 200:
            print(" ERROR: Something went wrong, will try again in 30 seconds.")
            response.raise_for_status()

        # get download size
        remaining_length = int(response.headers.get("content-length", 0))
        print(f"{filename.name} {first_byte=} {remaining_length=}")
        if remaining_length == 0:
            return

        # define chunk_size
        chunk_size = 8192

        # actual download
        with open(filename, "ab") as file:
            for chunk in response.iter_content(chunk_size):
                if chunk:
                    file.write(chunk)
                    #pbar.update(len(chunk))
                    #print(f"reading {filename.name} {len(chunk)}")
                else:
                    print(f"reading {filename.name} empty chunk")
        print(f"{filename.name} downloaded, {filename.stat().st_size=}")

    logger.info(f"Checking zip archive {filename.name} for consistency")
    zip_test = h.check_zipfile(filename)

    # if it did not pass the test, remove the file
    # in the while loop it will be downloaded again
    if zip_test is not None:
        logger.info(f"{filename.name} did not pass the zip test. Re-downloading " f"the full scene.")
        #filename.unlink()
        #first_byte = 0
        raise ValueError(f"zip test failed for {filename.name}")
    # otherwise we change the status to downloaded
    logger.info(f"{filename.name} passed the zip test.")
    with open(filename.with_suffix(".downloaded"), "w") as file:
        file.write("successfully downloaded \n")


def s1_download_parallel(argument_list):
    """Helper function for parallel download from scihub"""

    uuid, filename, uname, pword, base_url = argument_list
    s1_download(uuid, filename, uname, pword, base_url)


def batch_download(
    inventory_df,
    download_dir,
    uname,
    pword,
    concurrent=2,
    base_url="https://catalogue.dataspace.copernicus.eu",
):
    """Batch download Sentinel-1 on the basis of an OST inventory GeoDataFrame

    :param inventory_df:
    :param download_dir:
    :param uname:
    :param pword:
    :param concurrent:
    :param base_url:

    :return:
    """
    from ost import Sentinel1Scene as S1Scene

    if isinstance(download_dir, str):
        download_dir = Path(download_dir)

    # create list of scenes
    scenes = inventory_df["identifier"].tolist()

    check, i = False, 1
    while check is False and i <= 10:

        download_list = []
        for scene_id in scenes:
            scene = S1Scene(scene_id)
            filepath = scene.download_path(download_dir, True)
            if Path(f"{filepath}.downloaded").exists():
                logger.debug(f"{scene.scene_id} is already downloaded.")
            else:
                try:
                    uuid = inventory_df["uuid"][inventory_df["identifier"] == scene_id].tolist()
                except KeyError:
                    #uuid = [scene.scihub_uuid(connect(uname=uname, pword=pword, base_url=base_url))]
                    print("cannot find uuid in inventory " + str(inventory_df))
                    raise
                # create list objects for download
                download_list.append([uuid[0], filepath, uname, pword, base_url])

        if download_list:
            pool = multiprocessing.Pool(processes=concurrent)
            pool.map(s1_download_parallel, download_list)

        downloaded_scenes = list(download_dir.glob("**/*.downloaded"))

        if len(inventory_df["identifier"].tolist()) == len(downloaded_scenes):
            logger.info("All products are downloaded.")
            check = True
        else:
            check = False
            for scene in scenes:

                scene = S1Scene(scene)
                file_path = scene.download_path(download_dir)

                if file_path.with_suffix(".downloaded").exists():
                    scenes.remove(scene.scene_id)

        i += 1


def check_connection(uname, pword, base_url="https://catalogue.dataspace.copernicus.eu"):
    """Check if a connection with CDSE can be established
    :param uname:
    :param pword:
    :param base_url:
    :return:
    """
    access_token = get_access_token(uname, pword)
    # we use some random url for checking (also for czech mirror)
    url = f"{base_url}/odata/v1/Products(8f30a536-c01c-4ef4-ac74-be3378dc44c4)/$value"
    with requests.Session() as session:
        headers = {'Authorization': f'Bearer {access_token}'}
        request = session.request("head", url)
        response = session.get(request.url, headers=headers, stream=True)
    return response.status_code
