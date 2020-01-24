
'''
Based on a set of search parameters the script will create a query
on www.scihub.copernicus.eu and return the results either
as shapefile, sqlite, or write to a PostGreSQL database.

------------------
Usage
------------------

python3 search.py -a /path/to/aoi-shapefile.shp -b 2018-01-01 -e 2018-31-12
                   -t GRD -m VV -b IW -o /path/to/search.shp

    -a         defines ISO3 country code or path to an ESRI shapefile
    -s         defines the satellite platform (Sentinel-1, Sentinel-2, etc.)
    -b         defines start date*
    -e         defines end date for search*
    -t         defines the product type (i.e. RAW,SLC or GRD)*
    -m         defines the polarisation mode (VV, VH, HH or HV)*
    -b         defines the beammode (IW,EW or SM)*
    -o         defines output that can be a shapefile (ending with .shp),
               a SQLite DB (ending with .sqlite) or a PostGreSQL DB (no suffix)
    -u         the scihub username*
    -p         the scihub secret password*

    * optional, i.e will look for all available products as well as ask for
      username and password during script execution
'''

import os
import sys
import logging
import datetime
from urllib.error import URLError
import xml.dom.minidom
import dateutil.parser

import geopandas as gpd
from shapely.wkt import dumps, loads

from ost.helpers.db import pgHandler
from ost.helpers import scihub

logger = logging.getLogger(__name__)


def _query_scihub(apihub, opener, query):
    """
    Get the data from the scihub catalogue
    and write it to a GeoPandas GeoDataFrame
    """

    # create empty GDF
    columns = [
        'identifier', 'polarisationmode', 'orbitdirection',
        'acquisitiondate', 'relativeorbitnumber', 'orbitnumber',
        'producttype', 'slicenumber', 'size', 'beginposition',
        'endposition', 'lastrelativeorbitnumber', 'lastorbitnumber',
        'uuid', 'platformidentifier', 'missiondatatakeid',
        'swathidentifier', 'ingestiondate', 'sensoroperationalmode',
        'footprint'
        ]

    crs = {'init': 'epsg:4326'}
    geo_df = gpd.GeoDataFrame(columns=columns,
                              crs=crs,
                              geometry='footprint'
                              )
    dom = None
    # we need this for the paging
    index = 0
    rows = 99
    next_page = 1
    while next_page:
        # construct the final url
        url = apihub + query + "&rows={}&start={}".format(rows, index)
        try:
            # get the request
            req = opener.open(url)
            response = req.read().decode('utf-8')
            if response == '':
                raise ConnectionError('No response or page empty!!')
            dom = xml.dom.minidom.parseString(response)
        except URLError as err:
            if hasattr(err, 'reason'):
                logger.debug('We failed to connect to the server.')
                logger.debug('Reason: ', err.reason)
                sys.exit()
            elif hasattr(err, 'code'):
                logger.debug('The server couldn\'t fulfill the request.')
                logger.debug('Error code: ', err.code)
                sys.exit()
        if dom is None:
            return 'empty'
        acq_list = []
        # loop thorugh each entry (with all metadata)
        for node in dom.getElementsByTagName('entry'):
            # we get all the date entries
            dict_date = {
                s.getAttribute('name'):
                    dateutil.parser.parse(s.firstChild.data).astimezone(
                        dateutil.tz.tzutc()
                    )
                for s in node.getElementsByTagName('date')
            }

            # we get all the int entries
            dict_int = {
                s.getAttribute('name'): s.firstChild.data
                for s in node.getElementsByTagName('int')
            }

            # we create a filter for the str entries (we do not want all) and get them
            dict_str = {
                s.getAttribute('name'): s.firstChild.data
                for s in node.getElementsByTagName('str')
            }

            # merge the dicts and append to the catalogue list
            acq = dict(dict_date, **dict_int, **dict_str)

            # fill in emtpy fields in dict by using identifier
            if 'swathidentifier'not in acq.keys():
                acq['swathidentifier'] = acq['identifier'].split("_")[1]
            if 'producttype'not in acq.keys():
                acq['producttype'] = acq['identifier'].split("_")[2]
            if 'slicenumber'not in acq.keys():
                acq['slicenumber'] = 0
            # append all scenes from this page to a list
            acq_list.append([acq['identifier'],
                             acq['polarisationmode'],
                             acq['orbitdirection'],
                             acq['beginposition'].strftime('%Y%m%d'),
                             acq['relativeorbitnumber'],
                             acq['orbitnumber'],
                             acq['producttype'],
                             acq['slicenumber'],
                             acq['size'],
                             acq['beginposition'].isoformat(),
                             acq['endposition'].isoformat(),
                             acq['lastrelativeorbitnumber'],
                             acq['lastorbitnumber'],
                             acq['uuid'],
                             acq['platformidentifier'],
                             acq['missiondatatakeid'],
                             acq['swathidentifier'],
                             acq['ingestiondate'].isoformat(),
                             acq['sensoroperationalmode'],
                             loads(acq['footprint'])
                             ])

        # transofmr all results from that page to a gdf
        gdf = gpd.GeoDataFrame(acq_list,
                               columns=columns,
                               crs=crs,
                               geometry='footprint'
                               )

        # append the gdf to the full gdf
        geo_df = geo_df.append(gdf)

        # retrieve next page and set index up by 99 entries
        next_page = scihub.next_page(dom)
        index += rows

    return geo_df


def _to_shapefile(gdf, outfile, append=False):

    # check if file is there
    if os.path.isfile(outfile):
        # in case we want to append, we load the old one and add the new one
        if append:
            columns = [
                'id', 'identifier', 'polarisationmode',
                'orbitdirection', 'acquisitiondate', 'relativeorbit',
                'orbitnumber', 'product_type', 'slicenumber', 'size',
                'beginposition', 'endposition',
                'lastrelativeorbitnumber', 'lastorbitnumber',
                'uuid', 'platformidentifier', 'missiondatatakeid',
                'swathidentifier', 'ingestiondate',
                'sensoroperationalmode', 'geometry'
            ]

            # get existing geodataframe from file
            old_df = gpd.read_file(outfile)
            old_df.columns = columns
            # drop id
            old_df.drop('id', axis=1, inplace=True)
            # append new results
            gdf.columns = columns[1:]
            gdf = old_df.append(gdf)

            # remove duplicate entries
            gdf.drop_duplicates(subset='identifier', inplace=True)

        # remove old file
        os.remove(outfile)
        os.remove('{}.cpg'.format(outfile[:-4]))
        os.remove('{}.prj'.format(outfile[:-4]))
        os.remove('{}.shx'.format(outfile[:-4]))
        os.remove('{}.dbf'.format(outfile[:-4]))

    # calculate new index
    gdf.insert(loc=0, column='id', value=range(1, 1 + len(gdf)))

    # write to new file
    gdf.to_file(outfile)
    return outfile


def _to_postgis(gdf, db_connect, outtable):

    # check if tablename already exists
    db_connect.cursor.execute('SELECT EXISTS (SELECT * FROM '
                              'information_schema.tables WHERE '
                              'LOWER(table_name) = '
                              'LOWER(\'{}\'))'.format(outtable))
    result = db_connect.cursor.fetchall()
    if result[0][0] is False:
        logger.debug('INFO: Table {} does not exist in the database.'
                     'Creating it...'.format(outtable)
                     )
        db_connect.pgCreateS1('{}'.format(outtable))
        maxid = 1
    else:
        try:
            maxid = db_connect.pgSQL('SELECT max(id) FROM {}'.format(outtable))
            maxid = maxid[0][0]
            if maxid is None:
                maxid = 0

            logger.debug('INFO: Table {} already exists with {} entries. Will add'
                         'all non-existent results to this table.'.format(outtable,
                                                                          maxid
                                                                          )
                         )
            maxid = maxid + 1
        except:
            raise RuntimeError('ERROR: Existent table {} does not seem to be'
                               'compatible with Sentinel-1'
                               'data.'.format(outtable))

    # add an index as first column
    gdf.insert(loc=0, column='id', value=range(maxid, maxid + len(gdf)))
    db_connect.pgSQLnoResp('SELECT UpdateGeometrySRID(\'{}\', '
                           '\'geometry\', 0);'.format(outtable.lower()))

    # construct the SQL INSERT line
    for _index, row in gdf.iterrows():

        row['geometry'] = dumps(row['footlogger.debug'])
        row.drop('footlogger.debug', inplace=True)
        identifier = row.identifier
        uuid = row.uuid
        line = tuple(row.tolist())

        # first check if scene is already in the table
        result = db_connect.pgSQL('SELECT uuid FROM {} WHERE '
                                  'uuid = \'{}\''.format(outtable, uuid))
        try:
            test_query = result[0][0]
        except IndexError:
            logger.debug('Inserting scene {} to {}'.format(identifier, outtable))
            db_connect.pgInsert(outtable, line)
            # apply the dateline correction routine
            db_connect.pgDateline(outtable, uuid)
            maxid += 1
        else:
            logger.debug('Scene {} already exists within table {}.'.format(identifier,
                                                                    outtable))

    logger.debug('INFO: Inserted {} entries into {}.'.format(len(gdf), outtable))
    logger.debug('INFO: Table {} now contains {} entries.'.format(outtable,
                                                            maxid - 1))
    logger.debug('INFO: Optimising database table.')

    # drop index if existent
    try:
        db_connect.pgSQLnoResp('DROP INDEX {}_gix;'.format(outtable.lower()))
    except:
        pass

    # create geometry index and vacuum analyze
    db_connect.pgSQLnoResp('SELECT UpdateGeometrySRID(\'{}\', '
                           '\'geometry\', 4326);'.format(outtable.lower()))
    db_connect.pgSQLnoResp('CREATE INDEX {}_gix ON {} USING GIST '
                           '(geometry);'.format(outtable, outtable.lower()))
    db_connect.pgSQLnoResp('VACUUM ANALYZE {};'.format(outtable.lower()))


def check_availability(inventory_gdf, download_dir, data_mount):
    '''This function checks if the data is already downloaded or 
       available through a mount point on DIAS cloud
    
    '''
    
    from ost import Sentinel1Scene
    
    # add download path, or set to None if not found
    inventory_gdf['download_path'] = inventory_gdf.identifier.apply(
        lambda row: Sentinel1Scene(row).get_path(download_dir, data_mount))
    
    return inventory_gdf


def scihub_catalogue(
        query_string,
        output,
        append=False,
        uname=None,
        pword=None
):
    '''This is the main search function on scihub


    '''
    # retranslate Path object to string
    output = str(output)

    # get connected to scihub
    base_url = 'https://scihub.copernicus.eu/dhus/'
    opener = scihub.connect(base_url, uname, pword)
    action = 'search?q='
    apihub = base_url + action

    # get the catalogue in a dict
    gdf = _query_scihub(apihub, opener, query_string)

    # define output
    if output[-7:] == ".sqlite":
        logger.debug('INFO: writing to an sqlite file')
        # gdfInv2Sqlite(gdf, output)
    elif output[-4:] == ".shp":
        logger.debug('INFO: writing inventory data to shape file: {}'.format(output))
        _to_shapefile(gdf, output, append)
    else:
        logger.debug('INFO: writing inventory data toPostGIS table: {}'.format(output))
        db_connect = pgHandler()
        _to_postgis(gdf, db_connect, output)
    return output


if __name__ == "__main__":
    import argparse
    from ost.helpers import helpers

    # get the current date
    NOW = datetime.datetime.now()
    NOW = NOW.strftime("%Y-%m-%d")

    # write a description
    DESCRIPT = """
               This is a command line client for the inventory of Sentinel-1
               data on the Copernicus Scihub server.
               Output can be either an:
                    - exisiting PostGreSQL database
                    - newly created or existing SqLite database
                    - ESRI Shapefile
               """

    EPILOG = """
             Examples:
             search.py -a /path/to/aoi-shapefile.shp -b 2018-01-01
                       -e 2018-31-12
             """
    # create a PARSER
    PARSER = argparse.ArgumentParser(description=DESCRIPT, epilog=EPILOG)

    # username/password scihub
    PARSER.add_argument("-user", "--username",
                        help="Your username of scihub.copernicus.eu ",
                        default=None
                        )
    PARSER.add_argument("-pass", "--password",
                        help="Your secret password of scihub.copernicus.eu ",
                        default=None
                        )
    PARSER.add_argument("-a", "--areaofinterest",
                        help=('The Area of Interest as a WKT geometry'),
                        dest='aoi', default='*',
                        )
    PARSER.add_argument("-b", "--begindate",
                        help="The Start Date (format: YYYY-MM-DD) ",
                        default="2014-10-01",
                        type=lambda x: helpers.is_valid_date(PARSER, x)
                        )
    PARSER.add_argument("-e", "--enddate",
                        help="The End Date (format: YYYY-MM-DD)",
                        default=NOW,
                        type=lambda x: helpers.is_valid_date(PARSER, x)
                        )
    PARSER.add_argument("-t", "--producttype",
                        help="The Product Type (RAW, SLC, GRD, *) ",
                        default='*'
                        )
    PARSER.add_argument("-p", "--polarisation",
                        help="The Polarisation Mode (VV, VH, HH, HV, *) ",
                        default='*'
                        )
    PARSER.add_argument("-m", "--beammode",
                        help="The Beam Mode (IW, EW, SM, *) ",
                        default='*'
                        )

    # output parameters
    PARSER.add_argument("-o", "--output",
                        help=('Output format/file. Can be a shapefile'
                              '(ending with .shp), a SQLite file'
                              '(ending with .sqlite) or a PostGreSQL table'
                              '(connection needs to be configured). '
                              ),
                        required=True
                        )

    ARGS = PARSER.parse_args()
    # construct the search command (do not change)
    AOI = scihub.create_aoi_str(ARGS.aoi)
    TOI = scihub.create_toi_str(ARGS.begindate, ARGS.enddate)
    PRODUCT_SPECS = scihub.create_s1_product_specs(ARGS.producttype,
                                                   ARGS.polarisation,
                                                   ARGS.beammode
                                                   )

    QUERY = scihub.create_query('Sentinel-1', AOI, TOI, PRODUCT_SPECS)
    # execute full search
    out_destination = scihub_catalogue(QUERY,
                                       ARGS.output,
                                       append=False,
                                       uname=ARGS.username,
                                       pword=ARGS.password
                                       )
    print('DONE: Output written to "%s"' % out_destination)
