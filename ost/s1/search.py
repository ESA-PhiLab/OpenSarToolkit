#! /usr/bin/env python3

'''
Based on a set of search parameters the script will create a query
on www.scihub.copernicus.eu and return the results either
as shapefile, sqlite, or write to a PostGreSQL database.

----------------
Functions:
----------------
    scihubConnect:
        creates an urllib opener object for authentication on scihub server
    nextPage:
        gets the next page from a multi-page result from a scihub search
    createAoiWkt:
        creates a WKT representation of the AOI needed for the search query
    createToiStr:
        creates the string for the time of interest
    createSatStr:
        creates the satellite string for the search query
    createS1ProdSpecs:
        creates the product specific search string for Sentiel-1 search query
    createQuery:
        creates a string in the Open Search format that is added to the
        base scihub url
    getS1cat2Gdf:
        applies the search and writes the reults in a Geopandas GeoDataFrame
    gdfInv2Shp:
        writes the search result into an ESRI Shapefile
    gdfInv2Pg:
        writes the search result into a PostGreSQL/PostGIS Database
    gdfInv2Sqlite: (tba)
        writes the search result into a SqLite/SpatiaLite Database

------------------
Main function
------------------
  scihubSearch:
    handles the whole search process, i.e. login, query creation, search
    and write to desired output format

------------------
Contributors
------------------

Andreas Vollrath, ESA phi-lab
-----------------------------------
August 2018: Original implementation

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

# import stdlib modules
import os
import sys
import datetime
#import urllib
from urllib.error import URLError
import xml.dom.minidom
import dateutil.parser

# import external modules
import geopandas as gpd
from shapely.wkt import dumps, loads

# internal libs
from ost.helpers.db import pgHandler
from ost.helpers import scihub


def getS1Cat2Gdf(apihub, opener, query):
    """
    Get the data from the scihub catalogue
    and write it to a GeoPandas GeoDataFrame
    """

    # create empty GDF
    colNames = ['identifier', 'polarisationmode', 'orbitdirection',
                'acquisitiondate', 'relativeorbitnumber', 'orbitnumber',
                'producttype', 'slicenumber', 'size', 'beginposition',
                'endposition', 'lastrelativeorbitnumber', 'lastorbitnumber',
                'uuid', 'platformidentifier', 'missiondatatakeid',
                'swathidentifier', 'ingestiondate', 'sensoroperationalmode',
                'footprint']
    crs = {'init': 'epsg:4326'}
    gdfFull = gpd.GeoDataFrame(columns=colNames, crs=crs,
                               geometry='footprint')

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
        except URLError as e:
            if hasattr(e, 'reason'):
                print(' We failed to connect to the server.')
                print(' Reason: ', e.reason)
                sys.exit()
            elif hasattr(e, 'code'):
                print(' The server couldn\'t fulfill the request.')
                print(' Error code: ', e.code)
                sys.exit()
        else:
            # write the request to to the response variable 
            # (i.e. the xml coming back from scihub)
            response = req.read().decode('utf-8')

            # parse the xml page from the response
            dom = xml.dom.minidom.parseString(response)

        acqList = []
        # loop thorugh each entry (with all metadata)
        for node in dom.getElementsByTagName('entry'):

            # we get all the date entries
            dict_date = {s.getAttribute('name'):dateutil.parser.parse(s.firstChild.data).astimezone(dateutil.tz.tzutc()) for s in node.getElementsByTagName('date')}

            # we get all the int entries
            dict_int = {s.getAttribute('name'):s.firstChild.data for s in node.getElementsByTagName('int')}

            # we create a filter for the str entries (we do not want all) and get them
            dict_str = {s.getAttribute('name'):s.firstChild.data for s in node.getElementsByTagName('str')}
            
            # merge the dicts and append to the catalogue list
            acq = dict(dict_date,**dict_int,**dict_str)

            # fill in emtpy fields in dict by using identifier
            if not 'swathidentifier' in acq.keys():
                acq['swathidentifier'] = acq['identifier'].split("_")[1]
            if not 'producttype' in acq.keys():
                acq['producttype'] = acq['identifier'].split("_")[2]
            if not 'slicenumber' in acq.keys():
                acq['slicenumber'] = 0

            # append all scenes from this page to a list
            acqList.append([acq['identifier'], acq['polarisationmode'], 
                    acq['orbitdirection'], acq['beginposition'].strftime('%Y%m%d'),
                    acq['relativeorbitnumber'], acq['orbitnumber'], 
                    acq['producttype'],acq['slicenumber'], acq['size'],
                    acq['beginposition'].isoformat(), acq['endposition'].isoformat(), 
                    acq['lastrelativeorbitnumber'], acq['lastorbitnumber'],
                    acq['uuid'], acq['platformidentifier'], 
                    acq['missiondatatakeid'], acq['swathidentifier'], 
                    acq['ingestiondate'].isoformat(), acq['sensoroperationalmode'],
                    loads(acq['footprint'])])
               
        # transofmr all results from that page to a gdf
        gdf = gpd.GeoDataFrame(acqList, columns=colNames, crs=crs, geometry='footprint')
        
        # append the gdf to the full gdf
        gdfFull = gdfFull.append(gdf)
        
        # retrieve next page and set index up by 99 entries
        next_page = scihub.nextPage(dom)
        index += rows
    
    return gdfFull


def gdfS1Inv2Shp(gdf, outPath):
    
    # check if file is there
    #if os.path.isfile(outPath) is True:
        
     #   print(' ERROR: Output file already exists.')
     #  sys.exit()
        
#        maxid = 0
#        # in case check for maximum id
#        with fiona.open(outPath) as source:
#            for f in source:
#                print('id:', f['id'])
#                if maxid < int(f['id']):
#                    maxid = int(f['id'])
#        
#        print(maxid)
#        source.close()
#        
#        # calculate new index
#        gdf.insert(loc=0, column='id', value=range(maxid + 2 , 
#                                                   maxid + 2 + len(gdf)))
#        gdf.to_file(outPath, mode='a')
#   
    #else:
    
    # calculate new index
    gdf.insert(loc=0, column='id', value=range(1, 1 + len(gdf)))
    # write to new file
    
    try:
        os.remove(outPath)
    except OSError:
        pass
    
    gdf.to_file(outPath)
  
    
def gdfS1Inv2Pg(gdf, dbConnect, outTable):
    
     # check if tablename already exists
    dbConnect.cursor.execute('SELECT EXISTS (SELECT * FROM '
                             'information_schema.tables WHERE '
                             'LOWER(table_name) = '
                             'LOWER(\'{}\'))'.format(outTable))
    result = dbConnect.cursor.fetchall()
    if result[0][0] is False:
        print( ' INFO: Table {} does not exist in the database.'
               ' Creating it...'.format(outTable))
        dbConnect.pgCreateS1('{}'.format(outTable))
        maxid = 1
    else:
        try:
            maxid = dbConnect.pgSQL('SELECT max(id) FROM {}'.format(outTable))
            maxid = maxid[0][0]
            if maxid is None:
                maxid = 0

            print(' INFO: Table {} already exists with {} entries. Will add'
                  ' all non-existent results to this table.'.format(outTable, 
                                                                    maxid))
            maxid = maxid + 1
        except:
            raise RuntimeError(' ERROR: Existent table {} does not seem to be'
                               ' compatible with Sentinel-1'
                               ' data.'.format(outTable))

    # add an index as first column
    gdf.insert(loc=0, column='id', value=range(maxid, maxid + len(gdf)))
    dbConnect.pgSQLnoResp('SELECT UpdateGeometrySRID(\'{}\', '
                          '\'geometry\', 0);'.format(outTable.lower()))
    
    # construct the SQL INSERT line
    for index, row in gdf.iterrows():
        
        row['geometry'] = dumps(row['footprint'])
        row.drop('footprint', inplace=True)
        identifier = row.identifier
        uuid = row.uuid
        line = tuple(row.tolist())
        
        # first check if scene is already in the table
        result = dbConnect.pgSQL('SELECT uuid FROM {} WHERE '
                                 'uuid = \'{}\''.format(outTable, uuid))
        try:
            test_query = result[0][0]
        except IndexError:
            print('Inserting scene {} to {}'.format(identifier, outTable))
            dbConnect.pgInsert(outTable, line)
            # apply the dateline correction routine
            dbConnect.pgDateline(outTable, uuid)
            maxid += 1
        else:
            print('Scene {} already exists within table {}.'.format(identifier, 
                                                                    outTable))
                        
    print( 'INFO: Inserted {} entries into {}.'.format(len(gdf), outTable))
    print( 'INFO: Table {} now contains {} entries.'.format(outTable, 
                                                            maxid - 1 ))
    print( 'INFO: Optimising database table.')
    
    # drop index if existent
    try:
        dbConnect.pgSQLnoResp('DROP INDEX {}_gix;'.format(outTable.lower()))
    except:
        pass
    
    # create geometry index and vacuum analyze
    dbConnect.pgSQLnoResp('SELECT UpdateGeometrySRID(\'{}\', '
                          '\'geometry\', 4326);'.format(outTable.lower()))
    dbConnect.pgSQLnoResp('CREATE INDEX {}_gix ON {} USING GIST '
                          '(geometry);'.format(outTable, outTable.lower())) 
    dbConnect.pgSQLnoResp('VACUUM ANALYZE {};'.format(outTable.lower()))
    
    
def s1Scihub(query, output, uname=None, pword=None):
    
    # retranslate Path object to string
    output = str(output)
    
    # get connected to scihub
    baseURL = 'https://scihub.copernicus.eu/apihub/'
    opener = scihub.scihubConnect(baseURL, uname, pword)
    action = 'search?q='
    apihub = baseURL + action
    
    # get the catalogue in a dict
    gdf = getS1Cat2Gdf(apihub, opener, query)

     # define output
    if output[-7:] == ".sqlite":
        print(' INFO: writing to an sqlite file')
        #gdfInv2Sqlite(gdf, output)
    elif output[-4:] == ".shp":
        print(' INFO: writing inventory data to shape file: {}'.format(output))
        gdfS1Inv2Shp(gdf, output)
    else:
        print(' INFO: writing inventory data toPostGIS'
              ' table: {}'.format(output))
        dbConnect = pgHandler()
        gdfS1Inv2Pg(gdf, dbConnect, output)
    
        
if __name__ == "__main__":

    import argparse
    from ost.helpers import helpers

    # get the current date
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d")

    # write a description
    descript = """
               This is a command line client for the inventory of Sentinel-1 data
               on the Copernicus Scihub server.
               Output can be either an:
                    - exisiting PostGreSQL database
                    - newly created or existing SqLite database
                    - ESRI Shapefile
               """

    epilog = """
             Examples:
             search.py -a /path/to/aoi-shapefile.shp -b 2018-01-01 
                       -e 2018-31-12
             """
    # create a parser
    parser = argparse.ArgumentParser(description=descript, epilog=epilog)

    # username/password scihub
    parser.add_argument("-u", "--username",
                        help=" Your username of scihub.copernicus.eu ",
                        default=None)
    parser.add_argument("-p", "--password",
                        help=" Your secret password of scihub.copernicus.eu ",
                        default=None)
    parser.add_argument("-a", "--areaofinterest",
                        help=(' The Area of Interest (path to a shapefile'
                              'or ISO3 country code)'),
                        dest='aoi', default='*',
                        type=lambda x: helpers.is_valid_aoi(parser, x))
    parser.add_argument("-b", "--begindate",
                        help=" The Start Date (format: YYYY-MM-DD) ",
                        default="2014-10-01",
                        type=lambda x: helpers.is_valid_date(parser, x))
    parser.add_argument("-e", "--enddate",
                        help=" The End Date (format: YYYY-MM-DD)",
                        default=now,
                        type=lambda x: helpers.is_valid_date(parser, x))
    parser.add_argument("-t", "--producttype",
                        help=" The Product Type (RAW, SLC, GRD, *) ",
                        default = '*')
    parser.add_argument("-m", "--polarisation",
                        help=" The Polarisation Mode (VV, VH, HH, HV, *) ",
                        default = '*')
    parser.add_argument("-b", "--beammode",
                        help=" The Beam Mode (IW, EW, SM, *) ",
                        default='*')

    # output parameters
    parser.add_argument("-o", "--output",
                        help=(' Output format/file. Can be a shapefile'
                              ' (ending with .shp), a SQLite file'
                              ' (ending with .sqlite) or a PostGreSQL table'
                              ' (connection needs to be configured). '),
                        required=True)

    args = parser.parse_args()

    # execute full search
    if args.aoi is not '*' and args.aoi[-4] is 'shp':
        aoi = os.path.abspath(args.aoi)
    else:
        aoi = '*'

    # construct the search command (do not change)
    aoiStr = scihub.createAoiWkt(aoi)
    toiStr = scihub.createToiStr(args.begindate, args.enddate)
    prodSpecsStr = scihub.createS1ProdSpecs(args.producttype, 
                                            args.polarisation, args.beam)
    query = scihub.createQuery('Sentinel-1', aoiStr, toiStr, prodSpecsStr)
    
    # execute full search
    s1Scihub(query, args.output, args.username, args.password)
