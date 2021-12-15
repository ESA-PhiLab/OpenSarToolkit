#! /usr/bin/env python
"""
This script allows for the search of Sentinel-1 data on scihub.

Based on some search parameters the script will create a query on
www.scihub.copernicus.eu and return the results either as shapefile,
sqlite, or PostGreSQL database.
"""

# import modules
import getpass
import os
import logging
import psycopg2 as pg
from osgeo import ogr

from ost.helpers.vector import get_proj4, reproject_geometry

logger = logging.getLogger(__name__)


# see if the pg-file is there
def pgHandler(dbConnectFile="{}/.phiSAR/pgdb".format(os.getenv("HOME"))):
    """
    This function connects to an existing PostGreSQL database,
    with the access parameters stored in the dbConnectFile as follows:

    "database name"
    "database user"
    "database password"
    "database host"
    "database port"

    :param dbConnectFile: path to the connect file
    :return: the psycopg2 database connection object
    """

    try:
        f = open(dbConnectFile)
    except (FileNotFoundError, IOError):
        logger.info(
            "ERROR: No PostGreSQL connection established. Make sure to configure a connection to phiSAR."
        )

    # read out dbname, username
    lines = f.read().splitlines()
    dbname = lines[0]
    uname = lines[1]
    pwDb = lines[2]
    host = lines[3]
    port = lines[4]

    logger.info("Connecting to PostGreSQL database: {}".format(dbname))
    dbConnect = pgConnect(uname, pwDb, dbname, host, port)

    return dbConnect


class pgConnect:
    def __init__(
        self, uname=None, pword=None, dbname="sat", host="localhost", port="5432"
    ):
        """
        Establish a connection to the Scihub-catalogue db
        """

        # ask for username and password in case you have not defined as command line options
        if uname is None:
            uname = input(" Your PostGreSQL database username:")
        if pword is None:
            pword = getpass.getpass(" Your PostGreSQL database password:")

        # try connecting
        try:
            self.connection = pg.connect(
                dbname=dbname, user=uname, host=host, password=pword, port=port
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
        except Exception:
            logger.info("Cannot connect to database")

    def pgCreateS1(self, tablename):

        f_list = "id serial PRIMARY KEY, identifier varchar(100), \
                   polarisation varchar(100), orbitdirection varchar(12), \
                   acquisitiondate date, relativeorbit smallint, \
                   orbitnumber integer, producttype varchar(4), \
                   slicenumber smallint, size varchar(12), \
                   beginposition timestamp, endposition timestamp, \
                   lastrelativeorbitnumber smallint, lastorbitnumber int, \
                   uuid varchar(40), platformidentifier varchar(10), \
                   missiondatatakeid integer, swathidentifer varchar(21), \
                   ingestiondate timestamp, sensoroperationalmode varchar(3), \
                   geometry geometry"

        sql_cmd = "CREATE TABLE {} ({})".format(tablename, f_list)
        self.cursor.execute(sql_cmd)

    def pgGetUUID(self, sceneID, tablename):

        sql_cmd = "SELECT uuid FROM {} WHERE identifier = '{}'".format(
            tablename, sceneID
        )
        self.cursor.execute(sql_cmd)
        uuid = self.cursor.fetchall()[0][0]
        return uuid

    def pgDrop(self, tablename):
        sql_cmd = "DROP TABLE {}".format(tablename)
        self.cursor.execute(sql_cmd)

    def pgInsert(self, tablename, values):
        """
        This function inserts a table into the connected database object.
        """
        sql_cmd = "INSERT INTO {} VALUES {}".format(tablename, values)
        self.cursor.execute(sql_cmd)

    def pgSQL(self, sql):
        """
        This is a wrapper for a sql input that does get all responses.
        """
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def pgSQLnoResp(self, sql):
        """
        This is a wrapper for a sql input that does not get any response.
        """
        self.cursor.execute(sql)

    def shpGeom2pg(self, aoi, tablename):
        """
        This function is a wrapper to import a shapefile geometry to a PostGreSQL database
        """

        sqlCmd = "DROP TABLE IF EXISTS {}".format(tablename)
        self.cursor.execute(sqlCmd)

        fList = "id smallint, geometry geometry"
        sqlCmd = "CREATE TABLE {} ({})".format(tablename, fList)
        self.cursor.execute(sqlCmd)

        prjFile = "{}.prj".format(aoi[:-4])
        inProj4 = get_proj4(prjFile)

        sf = ogr.Open(aoi)
        layer = sf.GetLayer(0)
        for i in range(layer.GetFeatureCount()):
            feature = layer.GetFeature(i)
            wkt = feature.GetGeometryRef().ExportToWkt()

            if inProj4 != "+proj=longlat +datum=WGS84 +no_defs":
                wkt = reproject_geometry(wkt, inProj4, 4326)

            wkt = "St_GeomFromText('{}', 4326)".format(wkt)
            values = "('{}', {})".format(i, wkt)
            sql_cmd = "INSERT INTO {} VALUES {}".format(tablename, values)
            self.cursor.execute(sql_cmd)

    def pgDateline(self, tablename, uuid):
        """
        This function splits the acquisition footprint
        into a geometry collection if it crosses the dateline
        """
        # edited after https://www.mundialis.de/update-for-our-maps-mundialis-application-solves-dateline-wrap/
        sql_cmd = "UPDATE {} SET (geometry) = \
                    (SELECT \
                        ST_SetSRID( \
                            ST_CollectionExtract( \
                                ST_AsText( \
                                    ST_Split( \
                                    ST_ShiftLongitude(geometry), \
                                    ST_SetSRID( \
                                    ST_MakeLine( \
                                        ST_MakePoint(180,-90), \
                                        ST_MakePoint(180,90) \
                                        ), \
                                    4326 \
                                    ) \
                                ) \
                            ), \
                            3 \
                        ), \
                        4326 \
                        ) geometry \
                    FROM {} \
                    WHERE uuid = '{}' \
                    ) \
                    WHERE uuid  = '{}' \
                    AND ( \
                        ST_Intersects( \
                            geometry, \
                            ST_SetSRID( \
                                ST_MakeLine( \
                                    ST_MakePoint(-90,-90), \
                                    ST_MakePoint(-90,90) \
                                ), \
                                4326 \
                            ) \
                        ) \
                        AND \
                        ST_Intersects( \
                            geometry, \
                            ST_SetSRID( \
                                ST_MakeLine( \
                                    ST_MakePoint(90,-90), \
                                    ST_MakePoint(90,90) \
                                ), \
                                4326 \
                            ) \
                        ) \
                    ) \
                    AND \
                        geometry IS NOT NULL".format(
            tablename, tablename, uuid, uuid
        )
        self.cursor.execute(sql_cmd)
