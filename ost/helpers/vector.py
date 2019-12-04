import os
import sys
from functools import partial

import ogr
import pyproj
import logging
import geopandas as gpd

from osgeo import osr
from shapely.ops import transform
from shapely.wkt import loads
from shapely.geometry import Point, Polygon, mapping, shape
from fiona import collection
from fiona.crs import from_epsg

logger = logging.getLogger(__name__)


def get_epsg(prjfile):
    '''Get the epsg code from a projection file of a shapefile

    Args:
        prjfile: a .prj file of a shapefile

    Returns:
        str: EPSG code

    '''

    prj_file = open(prjfile, 'r')
    prj_txt = prj_file.read()
    srs = osr.SpatialReference()
    srs.ImportFromESRI([prj_txt])
    srs.AutoIdentifyEPSG()
    # return EPSG code
    return srs.GetAuthorityCode(None)


def get_proj4(prjfile):
    '''Get the proj4 string from a projection file of a shapefile

    Args:
        prjfile: a .prj file of a shapefile

    Returns:
        str: PROJ4 code

    '''

    prj_file = open(prjfile, 'r')
    prj_string = prj_file.read()

    # Lambert error
    if '\"Lambert_Conformal_Conic\"'in prj_string:

        logger.debug('ERROR: It seems you used an ESRI generated shapefile'
                     'with Lambert Conformal Conic projection. '
                     )
        logger.debug('This one is not compatible with Open Standard OGR/GDAL'
                     'tools used here. '
                     )
        logger.debug('Reproject your shapefile to a standard Lat/Long projection'
                     'and try again'
                     )
        exit(1)

    srs = osr.SpatialReference()
    srs.ImportFromESRI([prj_string])
    return srs.ExportToProj4()


def reproject_geometry(geom, inproj4, out_epsg):
    '''Reproject a wkt geometry based on EPSG code

    Args:
        geom (ogr-geom): an ogr geom objecct
        inproj4 (str): a proj4 string
        out_epsg (str): the EPSG code to which the geometry should transformed

    Returns
        geom (ogr-geometry object): the transformed geometry

    '''

    geom = ogr.CreateGeometryFromWkt(geom)
    # input SpatialReference
    spatial_ref_in = osr.SpatialReference()
    spatial_ref_in.ImportFromProj4(inproj4)

    # output SpatialReference
    spatial_ref_out = osr.SpatialReference()
    spatial_ref_out.ImportFromEPSG(int(out_epsg))

    # create the CoordinateTransformation
    coord_transform = osr.CoordinateTransformation(spatial_ref_in,
                                                   spatial_ref_out)
    try:
        geom.Transform(coord_transform)
    except:
        logger.debug('ERROR: Not able to transform the geometry')
        sys.exit()

    return geom


def geodesic_point_buffer(lat, lon, meters, envelope=False):

    # get WGS 84 proj
    proj_wgs84 = pyproj.Proj(init='epsg:4326')

    # Azimuthal equidistant projection
    aeqd_proj = '+proj=aeqd +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0'
    project = partial(
        pyproj.transform,
        pyproj.Proj(aeqd_proj.format(lat=lat, lon=lon)),
        proj_wgs84)

    buf = Point(0, 0).buffer(meters)  # distance in metres

    if envelope is True:
        geom = Polygon(transform(project, buf).exterior.coords[:]).envelope
    else:
        geom = Polygon(transform(project, buf).exterior.coords[:])

    return geom.to_wkt()


def latlon_to_wkt(lat, lon, buffer_degree=None, buffer_meter=None, envelope=False):
    '''A helper function to create a WKT representation of Lat/Lon pair

    This function takes lat and lon vale and returns the WKT Point
    representation by default.

    A buffer can be set in metres, which returns a WKT POLYGON. If envelope
    is set to True, the buffer will be squared by the extent buffer radius.

    Args:
        lat (str): Latitude (deg) of a point
        lon (str): Longitude (deg) of a point
        buffer (float): optional buffer around the point
        envelope (bool): gives a square instead of a circular buffer
                         (only applies if bufferis set)

    Returns:
        wkt (str): WKT string

    '''

    if buffer_degree is None and buffer_meter is None:
        aoi_wkt = 'POINT ({} {})'.format(lon, lat)

    elif buffer_degree:
        aoi_geom = loads('POINT ({} {})'.format(lon, lat)).buffer(buffer_degree)
        if envelope:
            aoi_geom = aoi_geom.envelope

        aoi_wkt = aoi_geom.to_wkt()

    elif buffer_meter:
        aoi_wkt = geodesic_point_buffer(lat, lon, buffer_meter, envelope)

    return aoi_wkt


def wkt_manipulations(wkt, buffer=None, convex=False, envelope=False):

    geom = ogr.CreateGeometryFromWkt(wkt)

    if buffer:
        geom = geom.Buffer(buffer)

    if convex:
        geom = geom.ConvexHull()

    if envelope:
        geom = geom.GetEnvelope()
        geom = ogr.CreateGeometryFromWkt(
            'POLYGON (({} {}, {} {}, {} {}, {} {}, {} {}, {} {}))'.format(
                geom[1], geom[3], geom[0], geom[3], geom[0], geom[2],
                geom[1], geom[2], geom[1], geom[3], geom[1], geom[3]))

    return geom.ExportToWkt()


def shp_to_wkt(shapefile, buffer=None, convex=False, envelope=False):
    '''A helper function to translate a shapefile into WKT


    '''

    # get filepaths and proj4 string
    shpfile = os.path.abspath(shapefile)
    prjfile = shpfile[:-4] + '.prj'
    proj4 = get_proj4(prjfile)

    lyr_name = os.path.basename(shapefile)[:-4]
    shp = ogr.Open(os.path.abspath(shapefile))
    lyr = shp.GetLayerByName(lyr_name)
    geom = ogr.Geometry(ogr.wkbGeometryCollection)

    for feat in lyr:
        geom.AddGeometry(feat.GetGeometryRef())
        wkt = geom.ExportToWkt()

    if proj4 != '+proj=longlat +datum=WGS84 +no_defs':
        logger.debug('INFO: Reprojecting AOI file to Lat/Long (WGS84)')
        wkt = reproject_geometry(wkt, proj4, 4326).ExportToWkt()

    # do manipulations if needed
    wkt = wkt_manipulations(wkt, buffer=buffer, convex=convex,
                            envelope=envelope)

    return wkt


def kml_to_wkt(kmlfile):

    shp = ogr.Open(os.path.abspath(kmlfile))
    lyr = shp.GetLayerByName()
    for feat in lyr:
        geom = feat.GetGeometryRef()
    wkt = str(geom)

    return wkt


def latlon_to_shp(lon, lat, shapefile):

    shapefile = str(shapefile)

    schema = {'geometry': 'Point',
              'properties': {'id': 'str'}}

    wkt = loads('POINT ({} {})'.format(lon, lat))

    with collection(shapefile, "w",
                    crs=from_epsg(4326),
                    driver="ESRI Shapefile",
                    schema=schema) as output:

        output.write({'geometry': mapping(wkt),
                      'properties': {'id': '1'}})


def shp_to_gdf(shapefile):

    gdf = gpd.GeoDataFrame.from_file(shapefile)

    prjfile = shapefile[:-4] + '.prj'
    proj4 = get_proj4(prjfile)

    if proj4 != '+proj=longlat +datum=WGS84 +no_defs':
        logger.debug('INFO: reprojecting AOI layer to WGS84.')
        # reproject
        gdf.crs = (proj4)
        gdf = gdf.to_crs({'init': 'epsg:4326'})

    return gdf


def wkt_to_gdf(wkt):

    if loads(wkt).geom_type == 'Point':
        data = {'id': ['1'],
                'geometry': loads(wkt).buffer(0.05).envelope}
        gdf = gpd.GeoDataFrame(data)

    elif loads(wkt).geom_type == 'Polygon':
        data = {'id': ['1'],
                'geometry': loads(wkt)}
        gdf = gpd.GeoDataFrame(data)

    elif loads(wkt).geom_type == 'GeometryCollection'and len(loads(wkt)) == 1:

        data = {'id': ['1'],
                'geometry': loads(wkt)}
        gdf = gpd.GeoDataFrame(data)
    else:

        i, ids, geoms = 1, [], []
        for geom in loads(wkt):
            ids.append(i)
            geoms.append(geom)
            i += 1

        data = {'id': ['1'],
                'geometry': loads(wkt[0])}
        gdf = gpd.GeoDataFrame(data)

    gdf.crs = {'init': 'epsg:4326',  'no_defs': True}

    return gdf


def wkt_to_shp(wkt, outfile):

    gdf = wkt_to_gdf(wkt)
    gdf.to_file(outfile)


def inventory_to_shp(inventory_df, outfile):

    # change datetime datatypes
    inventory_df['acquisitiondate'] = inventory_df[
        'acquisitiondate'].astype(str)
    inventory_df['ingestiondate'] = inventory_df['ingestiondate'].astype(str)
    inventory_df['beginposition'] = inventory_df['beginposition'].astype(str)
    inventory_df['endposition'] = inventory_df['endposition'].astype(str)

    # write to shapefile
    inventory_df.to_file(outfile)


def exterior(infile, outfile, buffer=None):

    gdf = gpd.read_file(infile, crs={'init': 'EPSG:4326'})
    gdf.geometry = gdf.geometry.apply(lambda row: Polygon(row.exterior))
    gdf_clean = gdf[gdf.geometry.area >= 1.0e-6]
    gdf_clean.geometry = gdf_clean.geometry.buffer(-0.0018)
    # if buffer:
    #    gdf.geometry = gdf.geometry.apply(
    #           lambda row: Polygon(row.buffer(-0.0018)))
    gdf_clean.to_file(outfile)


def difference(infile1, infile2, outfile):

    gdf1 = gpd.read_file(infile1)
    gdf2 = gpd.read_file(infile2)

    gdf3 = gpd.overlay(gdf1, gdf2, how='symmetric_difference')

    gdf3.to_file(outfile)


def buffer_shape(infile, outfile, buffer=None):

    with collection(infile, "r") as in_shape:
        # schema = in_shape.schema.copy()
        schema = {'geometry': 'Polygon', 'properties': {'id': 'int'}}
        crs = in_shape.crs
        with collection(
                outfile, "w", "ESRI Shapefile", schema, crs=crs) as output:

            for i, point in enumerate(in_shape):
                output.write({
                    'properties': {
                        'id': i
                    },
                    'geometry': mapping(
                        shape(point['geometry']).buffer(buffer))
                })


def plot_inventory(aoi, inventory_df, transperancy=0.05):

    import matplotlib.pyplot as plt

    # load world borders for background
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

    # import aoi as gdf
    aoi_gdf = wkt_to_gdf(aoi)

    # get bounds of AOI
    bounds = inventory_df.geometry.bounds

    # get world map as base
    base = world.plot(color='lightgrey', edgecolor='white')

    # plot aoi
    aoi_gdf.plot(ax=base, color='None', edgecolor='black')

    # plot footlogger.debugs
    inventory_df.plot(ax=base, alpha=transperancy)

    # set bounds
    plt.xlim([bounds.minx.min()-2, bounds.maxx.max()+2])
    plt.ylim([bounds.miny.min()-2, bounds.maxy.max()+2])
    plt.grid(color='grey', linestyle='-', linewidth=0.2)
