#! /usr/bin/env python
# -*- coding: utf-8 -*-

# import stdlib modules
import os
import requests
import logging

from pathlib import Path

# import external modules
import pandas as pd
import geopandas as gpd
from shapely.wkt import dumps
from shapely.geometry import Polygon, shape
from tqdm import tqdm

# internal OST libs
from ost.helpers.db import pgHandler
from ost.helpers import copernicus as cop

# set up logger
logger = logging.getLogger(__name__)



def _to_shapefile(gdf, outfile, append=False):

    # check if file is there
    if os.path.isfile(outfile):

        # in case we want to append, we load the old one and add the new one
        if append:
            columns = [
                "id",
                "identifier",
                "polarisationmode",
                "orbitdirection",
                "acquisitiondate",
                "relativeorbit",
                "orbitnumber",
                "product_type",
                "slicenumber",
                "size",
                "beginposition",
                "endposition",
                "lastrelativeorbitnumber",
                "lastorbitnumber",
                "uuid",
                "platformidentifier",
                "missiondatatakeid",
                "swathidentifier",
                "ingestiondate",
                "sensoroperationalmode",
                "geometry",
            ]

            # get existing geodataframe from file
            old_df = gpd.read_file(outfile)
            old_df.columns = columns
            # drop id
            old_df.drop("id", axis=1, inplace=True)
            # append new results
            gdf.columns = columns[1:]
            gdf = old_df.append(gdf)

            # remove duplicate entries
            gdf.drop_duplicates(subset="identifier", inplace=True)

        # remove old file
        os.remove(outfile)
        os.remove("{}.cpg".format(outfile[:-4]))
        os.remove("{}.prj".format(outfile[:-4]))
        os.remove("{}.shx".format(outfile[:-4]))
        os.remove("{}.dbf".format(outfile[:-4]))

    # calculate new index
    gdf.insert(loc=0, column="id", value=range(1, 1 + len(gdf)))

    # write to new file
    if len(gdf.index) >= 1:
        gdf.to_file(outfile)
    else:
        logger.info("No scenes found in this AOI during this time")


def _to_geopackage(gdf, outfile, append=False):

    # check if file is there
    if Path(outfile).exists():

        # in case we want to append, we load the old one and add the new one
        if append:
            columns = [
                "id",
                "identifier",
                "polarisationmode",
                "orbitdirection",
                "acquisitiondate",
                "relativeorbit",
                "orbitnumber",
                "product_type",
                "slicenumber",
                "size",
                "beginposition",
                "endposition",
                "lastrelativeorbitnumber",
                "lastorbitnumber",
                "uuid",
                "platformidentifier",
                "missiondatatakeid",
                "swathidentifier",
                "ingestiondate",
                "sensoroperationalmode",
                "geometry",
            ]

            # get existing geodataframe from file
            old_df = gpd.read_file(outfile)
            old_df.columns = columns
            # drop id
            old_df.drop("id", axis=1, inplace=True)
            # append new results
            gdf.columns = columns[1:]
            gdf = old_df.append(gdf)

            # remove duplicate entries
            gdf.drop_duplicates(subset="identifier", inplace=True)

        # remove old file
        Path(outfile).unlink()

    # calculate new index
    gdf.insert(loc=0, column="id", value=range(1, 1 + len(gdf)))

    # write to new file
    if len(gdf.index) > 0:
        gdf.to_file(outfile, driver="GPKG")
    else:
        logger.info("No scenes found in this AOI during this time")


def _to_postgis(gdf, db_connect, outtable):

    # check if tablename already exists
    db_connect.cursor.execute(
        "SELECT EXISTS (SELECT * FROM "
        "information_schema.tables WHERE "
        "LOWER(table_name) = "
        "LOWER('{}'))".format(outtable)
    )
    result = db_connect.cursor.fetchall()
    if result[0][0] is False:
        logger.info(f"Table {outtable} does not exist in the database. Creating it...")
        db_connect.pgCreateS1("{}".format(outtable))
        maxid = 1
    else:
        try:
            maxid = db_connect.pgSQL(f"SELECT max(id) FROM {outtable}")
            maxid = maxid[0][0]
            if maxid is None:
                maxid = 0

            logger.info(
                f"Table {outtable} already exists with {maxid} entries. "
                f"Will add all non-existent results to this table."
            )
            maxid = maxid + 1
        except Exception:
            raise RuntimeError(
                f"Existent table {outtable} does not seem to be compatible " f"with Sentinel-1 data."
            )

    # add an index as first column
    gdf.insert(loc=0, column="id", value=range(maxid, maxid + len(gdf)))
    db_connect.pgSQLnoResp(f"SELECT UpdateGeometrySRID('{outtable.lower()}', 'geometry', 0);")

    # construct the SQL INSERT line
    for _index, row in gdf.iterrows():

        row["geometry"] = dumps(row["footprint"])
        row.drop("footprint", inplace=True)
        identifier = row.identifier
        uuid = row.uuid
        line = tuple(row.tolist())

        # first check if scene is already in the table
        result = db_connect.pgSQL("SELECT uuid FROM {} WHERE " "uuid = '{}'".format(outtable, uuid))
        try:
            result[0][0]
        except IndexError:
            logger.info(f"Inserting scene {identifier} to {outtable}")
            db_connect.pgInsert(outtable, line)
            # apply the dateline correction routine
            db_connect.pgDateline(outtable, uuid)
            maxid += 1
        else:
            logger.info(f"Scene {identifier} already exists within table {outtable}.")

    logger.info(f"Inserted {len(gdf)} entries into {outtable}.")
    logger.info(f"Table {outtable} now contains {maxid - 1} entries.")
    logger.info("Optimising database table.")

    # drop index if existent
    try:
        db_connect.pgSQLnoResp("DROP INDEX {}_gix;".format(outtable.lower()))
    except Exception:
        pass

    # create geometry index and vacuum analyze
    db_connect.pgSQLnoResp("SELECT UpdateGeometrySRID('{}', " "'geometry', 4326);".format(outtable.lower()))
    db_connect.pgSQLnoResp(
        "CREATE INDEX {}_gix ON {} USING GIST " "(geometry);".format(outtable, outtable.lower())
    )
    db_connect.pgSQLnoResp("VACUUM ANALYZE {};".format(outtable.lower()))


def check_availability(inventory_gdf, download_dir, data_mount):
    """This function checks if the data is already downloaded or
       available through a mount point on DIAS cloud

    :param inventory_gdf:
    :param download_dir:
    :param data_mount:
    :return:
    """

    from ost import Sentinel1Scene

    # add download path, or set to None if not found
    inventory_gdf["download_path"] = inventory_gdf.identifier.apply(
        lambda row: str(Sentinel1Scene(row).get_path(download_dir, data_mount))
    )

    return inventory_gdf

def transform_geometry(geometry):

    try:
        geom = Polygon(geometry['coordinates'][0])
    except:
        geom = Polygon(geometry['coordinates'][0][0])

    return geom


def query_dataspace(query, access_token):

    _next = query

    logger.info('Querying the Copernicus Dataspace Server for the search request')
    dfs, i = [], 1
    while _next:
        # get request
        json = requests.get(_next).json()
        #print(json)
        # append json outout to list of dataframes
        dfs.append(pd.DataFrame.from_dict(json['features']))
        try:
            _next = next(
                link['href'] for link in json['properties']['links'] if link['rel'] == 'next'
            )
            #_next = [link['href'] for link in json['properties']['links'] if link['rel'] == 'next'][0]
        except:
            _next = None
    df = pd.concat(dfs)

    if df.empty:
         raise ValueError('No products found for the given search parameters.')

    logger.info('Extracting basic metadata for the scenes')
    # extract basic metadata from retrieved json
    tqdm.pandas()
    df[[
        'identifier',
        'orbitdirection',
        'platformidentifier',
        'polarisationmode',
        'swathidentifier',
        'metafile',
        'ingestiondate'
    ]] = df.progress_apply(
            lambda x: cop.extract_basic_metadata(x['properties']), axis=1, result_type='expand'
    )


    # Rename the id column to 'uuid'
    df.rename(columns={'id': 'uuid'}, inplace=True)

    # turn geometry into shapely objects
    df['geometry']= df['geometry'].apply(lambda x: transform_geometry(x))
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='epsg:4326')

    logger.info('Extracting advanced metadata directly from the Copernicus dataspace server.')
    gdf[[
        'slicenumber', 'totalslicenumbers',
        'relativeorbitnumber', 'lastrelativeorbitnumber',
        'platformidentifier', 'missiondatatakeid',
        'sensoroperationalmode', 'producttype',
        'orbitnumber', 'lastorbitnumber',
        'beginposition', 'endposition', 'acquisitiondate',
        'size'
    ]] = gdf.progress_apply(
        lambda x: cop.get_advanced_metadata(
            x['metafile'], access_token
        ), axis=1, result_type='expand'
    )

    # add a unique id
    #gdf['id'] = [i + 1 for i in range(len(gdf))]

    # a list of columns to keep
    scihub_legacy_columns = [
        'identifier', 'polarisationmode', 'orbitdirection',
        'acquisitiondate', 'relativeorbitnumber', 'orbitnumber', 'producttype',
        'slicenumber', 'size', 'beginposition', 'endposition',
        'lastrelativeorbitnumber', 'lastorbitnumber', 'uuid',
        'platformidentifier', 'missiondatatakeid', 'swathidentifier',
        'ingestiondate', 'sensoroperationalmode', 'geometry'
    ]

    gdf = gdf[scihub_legacy_columns]
    return gdf


def dataspace_catalogue(
    query_string,
    output,
    append=False,
    uname=None,
    pword=None,
    base_url="https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel1/search.json?",
):
    """This is the main search function on scihub

    :param query_string:
    :param output:
    :param append:
    :param uname:
    :param pword:
    :param base_url:
    :return:
    """

    # retranslate Path object to string
    output = str(output)

    # get connected to scihub
    access_token = cop.get_access_token(uname, pword)
    query = base_url + query_string

    # get the catalogue in a dict
    gdf = query_dataspace(query, access_token)

    if output[-4:] == ".shp":
        logger.info(f"Writing inventory data to shape file: {output}")
        _to_shapefile(gdf, output, append)
    elif output[-5:] == ".gpkg":
        logger.info(f"Writing inventory data to geopackage file: {output}")
        _to_geopackage(gdf, output, append)
    else:
        logger.info(f"Writing inventory data toPostGIS table: {output}")
        db_connect = pgHandler()
        _to_postgis(gdf, db_connect, output)
