"""This module provides helper functions for Copernicus Dataspace API."""

import getpass
import logging
from pathlib import Path
from datetime import datetime as dt

import requests
from shapely.wkt import loads

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
