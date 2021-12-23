import os
import json
from pathlib import Path

import pyproj
from pyproj.crs import ProjectedCRS

# temporary, we need to set minimum version of pyproj
try:
    from pyproj.crs.coordinate_operation import AzimuthalEquidistantConversion
except ImportError:
    from pyproj.crs.coordinate_operation import (
        AzumuthalEquidistantConversion as AzimuthalEquidistantConversion,
    )

import geopandas as gpd
import logging
from osgeo import osr
from osgeo import ogr

from shapely.ops import transform
from shapely.wkt import loads
from shapely.geometry import Point, Polygon, mapping, shape
from shapely.errors import WKTReadingError
from fiona import collection
from fiona.crs import from_epsg

logger = logging.getLogger(__name__)


def aoi_to_wkt(aoi):
    """Helper function to transform various AOI formats into WKT

    This function is used to import an AOI definition into an OST project.
    The AOIs definition can be from difffrent sources, i.e. an ISO3 country
    code (that calls GeoPandas low-resolution country boundaries),
    a WKT string,

    :param aoi: AOI , which can be an ISO3 country code, a WKT String or
                a path to a shapefile, a GeoPackage or a GeoJSON file
    :type aoi: str/Path
    :return: AOI as WKT string
    :rtype: WKT string
    """

    # see if aoi alread in wkt
    try:
        # let's check if it is a shapely readable WKT
        loads(str(aoi))
        aoi_wkt = aoi
    except WKTReadingError:

        # see if aoi is an ISO3 country code
        try:
            # let's check if it is a shapely readable WKT
            world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
            aoi_wkt = world["geometry"][world["iso_a3"] == aoi].values[0].wkt

        except IndexError:
            # see if it is a Geovector file
            if Path(aoi).exists():
                try:
                    gdf = gpd.GeoDataFrame.from_file(aoi)
                    if gdf.crs != "epsg:4326":
                        try:
                            gdf = gdf.geometry.to_crs("epsg:4326")
                        except Exception:
                            raise ValueError("No valid OST AOI definition.")
                    # return AOI as single vector object
                    aoi_wkt = str(gdf.geometry.unary_union)
                except Exception:
                    # give up
                    raise ValueError("No valid OST AOI definition.")

            else:
                # give up
                raise ValueError("No valid OST AOI definition.")

    return aoi_wkt


def get_epsg(prjfile):
    """

    :param prjfile:
    :return:
    """

    prj_file = open(prjfile, "r")
    prj_txt = prj_file.read()
    srs = osr.SpatialReference()
    srs.ImportFromESRI([prj_txt])
    srs.AutoIdentifyEPSG()
    # return EPSG code
    return srs.GetAuthorityCode(None)


def get_proj4(prjfile):
    """Get the proj4 string from a projection file of a shapefile

    :param prjfile:
    :return:
    """

    prj_file = open(prjfile, "r")
    prj_string = prj_file.read()

    # Lambert error
    if '"Lambert_Conformal_Conic"' in prj_string:

        print(
            " ERROR: It seems you used an ESRI generated shapefile"
            " with Lambert Conformal Conic projection. "
        )
        print(
            " This one is not compatible with Open Standard OGR/GDAL"
            " tools used here. "
        )
        print(
            " Reproject your shapefile to a standard Lat/Long projection"
            " and try again"
        )
        exit(1)

    srs = osr.SpatialReference()
    srs.ImportFromESRI([prj_string])
    return srs.ExportToProj4()


def epsg_to_wkt_projection(epsg_code):
    """

    :param epsg_code:
    :return:
    """

    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromEPSG(epsg_code)

    return spatial_ref.ExpotToWkt()


def reproject_geometry(geom, inproj4, out_epsg):
    """Reproject a wkt geometry based on EPSG code

    :param geom: an ogr geom object
    :param inproj4: a proj4 string
    :param out_epsg: the EPSG code to which the geometry should transformed
    :return: the transformed geometry (ogr-geometry object)
    """

    geom = ogr.CreateGeometryFromWkt(geom)
    # input SpatialReference
    spatial_ref_in = osr.SpatialReference()
    spatial_ref_in.ImportFromProj4(inproj4)

    # output SpatialReference
    spatial_ref_out = osr.SpatialReference()
    spatial_ref_out.ImportFromEPSG(int(out_epsg))

    # create the CoordinateTransformation
    coord_transform = osr.CoordinateTransformation(spatial_ref_in, spatial_ref_out)
    try:
        geom.Transform(coord_transform)
    except Exception:
        raise RuntimeError(" ERROR: Not able to transform the geometry")

    return geom


def geodesic_point_buffer(lon, lat, meters, envelope=False):
    """

    :param lat:
    :param lon:
    :param meters:
    :param envelope:
    :return:
    """

    proj_crs = ProjectedCRS(
        conversion=AzimuthalEquidistantConversion(float(lat), float(lon))
    )

    proj_wgs84 = pyproj.Proj("EPSG:4326")

    Trans = pyproj.Transformer.from_proj(proj_crs, proj_wgs84, always_xy=True).transform

    buf = Point(0, 0).buffer(meters)  # distance in metres

    if envelope is True:
        geom = Polygon(transform(Trans, buf).exterior.coords[:]).envelope
    else:
        geom = Polygon(transform(Trans, buf).exterior.coords[:])

    return geom.wkt


def latlon_to_wkt(lat, lon, buffer_degree=None, buffer_meter=None, envelope=False):
    """A helper function to create a WKT representation of Lat/Lon pair

    This function takes lat and lon values and returns
    the WKT Point representation by default.

    A buffer can be set in meters, which returns a WKT POLYGON.
    If envelope is set to True, the circular buffer will be
    squared by the extent buffer radius.

    :param lat:
    :type lat: str
    :param lon:
    :type lon: str
    :param buffer_degree:
    :type buffer_degree: float, optional
    :param buffer_meter:
    :type buffer_meter: float, optional
    :param envelope:
    :type envelope: bool, optional

    :return: WKT string
    :rtype: str
    """

    if buffer_degree is None and buffer_meter is None:
        aoi_wkt = f"POINT ({lon} {lat})"

    elif buffer_degree:
        aoi_geom = loads(f"POINT ({lon} {lat})").buffer(buffer_degree)
        if envelope:
            aoi_geom = aoi_geom.envelope

        aoi_wkt = aoi_geom.wkt

    elif buffer_meter:
        aoi_wkt = geodesic_point_buffer(lon, lat, buffer_meter, envelope)

    return aoi_wkt


def wkt_manipulations(wkt, buffer=None, convex=False, envelope=False):
    """

    :param wkt:
    :param buffer:
    :param convex:
    :param envelope:
    :return:
    """

    geom = ogr.CreateGeometryFromWkt(wkt)

    if buffer:
        geom = geom.Buffer(buffer)

    if convex:
        geom = geom.ConvexHull()

    if envelope:
        geom = geom.GetEnvelope()
        geom = ogr.CreateGeometryFromWkt(
            f"POLYGON (("
            f"{geom[1]} {geom[3]}, "
            f"{geom[0]} {geom[3]}, "
            f"{geom[0]} {geom[2]}, "
            f"{geom[1]} {geom[2]}, "
            f"{geom[1]} {geom[3]}, "
            f"{geom[1]} {geom[3]}"
            f"))"
        )

    return geom.ExportToWkt()


def shp_to_wkt(shapefile, buffer=None, convex=False, envelope=False):
    """A helper function to translate a shapefile into WKT

    :param shapefile:
    :param buffer:
    :param convex:
    :param envelope:
    :return:
    """

    # get filepaths and proj4 string
    shpfile = os.path.abspath(shapefile)
    prjfile = shpfile[:-4] + ".prj"
    proj4 = get_proj4(prjfile)

    lyr_name = os.path.basename(shapefile)[:-4]
    shp = ogr.Open(os.path.abspath(shapefile))
    lyr = shp.GetLayerByName(lyr_name)
    geom = ogr.Geometry(ogr.wkbGeometryCollection)

    for feat in lyr:
        geom.AddGeometry(feat.GetGeometryRef())

    wkt = geom.ExportToWkt()

    if proj4 != "+proj=longlat +datum=WGS84 +no_defs":
        logger.info("Reprojecting AOI file to Lat/Long (WGS84)")
        wkt = reproject_geometry(wkt, proj4, 4326).ExportToWkt()

    # do manipulations if needed
    wkt = wkt_manipulations(wkt, buffer=buffer, convex=convex, envelope=envelope)

    return wkt


def latlon_to_shp(lon, lat, shapefile):
    """

    :param lon:
    :param lat:
    :param shapefile:
    :return:
    """

    shapefile = str(shapefile)

    schema = {"geometry": "Point", "properties": {"id": "str"}}

    wkt = loads("POINT ({} {})".format(lon, lat))

    with collection(
        shapefile, "w", crs=from_epsg(4326), driver="ESRI Shapefile", schema=schema
    ) as output:

        output.write({"geometry": mapping(wkt), "properties": {"id": "1"}})


def wkt_to_gdf(wkt):
    """

    :param wkt:
    :return:
    """

    # load wkt
    geometry = loads(wkt)

    # point wkt
    if geometry.geom_type == "Point":
        data = {"id": ["1"], "geometry": loads(wkt).buffer(0.05).envelope}
        gdf = gpd.GeoDataFrame(data)

    # polygon wkt
    elif geometry.geom_type == "Polygon":
        data = {"id": ["1"], "geometry": loads(wkt)}
        gdf = gpd.GeoDataFrame(data, crs="epsg:4326")

    # geometry collection of single multiploygon
    elif (
        geometry.geom_type == "GeometryCollection"
        and len(geometry) == 1
        and "MULTIPOLYGON" in str(geometry)
    ):

        data = {"id": ["1"], "geometry": geometry}
        gdf = gpd.GeoDataFrame(data, crs="epsg:4326")

        ids, feats = [], []
        for i, feat in enumerate(gdf.geometry.values[0]):
            ids.append(i)
            feats.append(feat)

        gdf = gpd.GeoDataFrame(
            {"id": ids, "geometry": feats}, geometry="geometry", crs=gdf.crs
        )

    # geometry collection of single polygon
    elif geometry.geom_type == "GeometryCollection" and len(geometry) == 1:

        data = {"id": ["1"], "geometry": geometry}
        gdf = gpd.GeoDataFrame(data, crs="epsg:4326")

    # everything else
    else:

        i, ids, geoms = 1, [], []
        for geom in geometry:
            ids.append(i)
            geoms.append(geom)
            i += 1

        gdf = gpd.GeoDataFrame({"id": ids, "geometry": geoms}, crs="epsg:4326")

    return gdf


def gdf_to_json_geometry(gdf):
    """Function to parse features from GeoDataFrame in such a manner
    that rasterio wants them"""

    geojson = json.loads(gdf.to_json())
    return [
        feature["geometry"] for feature in geojson["features"] if feature["geometry"]
    ]


def exterior(infile, outfile, buffer=None):
    """Creates an exterior vector of an input vector

    :param infile:
    :param outfile:
    :param buffer:
    :return:
    """
    gdf = gpd.read_file(infile, crs="epsg:4326")
    gdf.geometry = gdf.geometry.apply(lambda row: Polygon(row.exterior))
    gdf_clean = gdf[gdf.geometry.area >= 1.0e-6]

    if buffer:
        gdf_clean.geometry = gdf_clean.geometry.buffer(buffer)

    # a negative buffer might polygons make disappear, so let's clean them
    gdf_clean = gdf_clean[~gdf_clean.geometry.is_empty]

    gdf_clean.to_file(outfile, driver="GPKG")


def difference(infile1, infile2, outfile):

    import warnings

    warnings.filterwarnings("ignore", "Geometry is in a geographic CRS", UserWarning)

    gdf1 = gpd.read_file(infile1)
    gdf2 = gpd.read_file(infile2)

    gdf3 = gpd.overlay(gdf1, gdf2, how="difference")

    # remove slivers and artifacts
    gdf3 = gdf3.buffer(0)
    buffer = 0.00001
    gdf3 = gdf3.buffer(-buffer, 1, join_style=2).buffer(buffer, 1, join_style=2)
    gdf3.to_file(outfile, driver="GeoJSON")


def set_subset(aoi, inventory_df):

    # WKT aoi to shapely geom
    aoi = loads(aoi)

    # burst_inventory case
    if "bid" in inventory_df.columns:
        for burst in inventory_df.bid.unique():
            burst_geom = inventory_df.geometry[inventory_df.bid == burst].unary_union
            subset = True if aoi.within(burst_geom) else False
            if not subset:
                return subset

    # grd inventory case
    else:
        for track in inventory_df.relativeorbit.unique():
            track_geom = inventory_df.geometry[
                inventory_df.relativeorbit == track
            ].unary_union
            subset = True if aoi.within(track_geom) else False
            if not subset:
                return subset

    # return if true
    return subset


def buffer_shape(infile, outfile, buffer=None):

    with collection(infile, "r") as in_shape:
        schema = {"geometry": "Polygon", "properties": {"id": "int"}}
        crs = in_shape.crs
        with collection(outfile, "w", "ESRI Shapefile", schema, crs=crs) as output:

            for i, point in enumerate(in_shape):
                output.write(
                    {
                        "properties": {"id": i},
                        "geometry": mapping(shape(point["geometry"]).buffer(buffer)),
                    }
                )


def plot_inventory(aoi, inventory_df, transparency=0.05, annotate=False):

    import matplotlib.pyplot as plt

    # load world borders for background
    world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))

    # do the import of aoi as gdf
    aoi_gdf = wkt_to_gdf(aoi)

    # get bounds of AOI
    bounds = inventory_df.geometry.bounds

    # get world map as base
    base = world.plot(color="lightgrey", edgecolor="white")

    # plot aoi
    aoi_gdf.plot(ax=base, color="None", edgecolor="black")

    # plot footprints
    inventory_df.plot(ax=base, alpha=transparency)

    # set bounds
    plt.xlim([bounds.minx.min() - 2, bounds.maxx.max() + 2])
    plt.ylim([bounds.miny.min() - 2, bounds.maxy.max() + 2])
    plt.grid(color="grey", linestyle="-", linewidth=0.2)
    if annotate:
        import math

        for idx, row in inventory_df.iterrows():
            # print([row['geometry'].bounds[0],row['geometry'].bounds[3]])
            coord = [row["geometry"].centroid.x, row["geometry"].centroid.y]
            x1, y2, x2, y1 = row["geometry"].bounds
            angle = math.degrees(math.atan2((y2 - y1), (x2 - x1)))

            plt.annotate(
                s=row["bid"],
                xy=coord,
                rotation=angle + 5,
                size=10,
                color="red",
                horizontalalignment="center",
            )
