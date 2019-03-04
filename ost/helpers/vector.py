import os
import ogr
import fiona
from osgeo import osr
import geopandas as gpd

from shapely.geometry import mapping
from shapely.wkt import loads
from fiona import collection
from fiona.crs import from_epsg


def getEPSG(prjfile):
   """
   get the epsg code from a projection file of a shapefile
   """
   
   prj_file = open(prjfile, 'r')
   prj_txt = prj_file.read()
   srs = osr.SpatialReference()
   srs.ImportFromESRI([prj_txt])
   srs.AutoIdentifyEPSG()

   # return EPSG code
   return srs.GetAuthorityCode(None)


def getProj4(prjfile):
    """
    get the proj4 code from a projection file of a shapefile
    """

    prj_file = open(prjfile, 'r')
    prjTxt = prj_file.read()

    # Lambert error
    if '\"Lambert_Conformal_Conic\"' in prjTxt:
        
        print(' ERROR: It seems you used an ESRI generated shapefile'
              ' with Lambert Conformal Conic projection. ')
        print(' This one is not compatible with Open Standard OGR/GDAL'
              ' tools used here. ')
        print(' Reproject your shapefile to a standard Lat/Long projection'
              ' and try again')
        exit(1)

    srs = osr.SpatialReference()
    srs.ImportFromESRI([prjTxt])
    return srs.ExportToProj4()


def wktExtentFromShapefile(shpfile):
    """
    get the convex hull extent from a shapefile and return it as a WKT
    """

    print(' INFO: Calculating the convex hull of the given AOI file.')
    lyr_name = os.path.basename(shpfile)[:-4]
    shp = ogr.Open(os.path.abspath(shpfile))
    lyr = shp.GetLayerByName(lyr_name)
    geom = ogr.Geometry(ogr.wkbGeometryCollection)

    for feat in lyr:
       geom.AddGeometry(feat.GetGeometryRef())

    # create a convex hull
    geom = geom.ConvexHull()
    return geom.ExportToWkt()


def wktBoundingFromShapefile(shpfile):
    """
    get the convex hull extent from a shapefile and return it as a WKT
    """

    print(' INFO: Calculating the convex hull of the given AOI file.')
    lyr_name = os.path.basename(shpfile)[:-4]
    shp = ogr.Open(os.path.abspath(shpfile))
    lyr = shp.GetLayerByName(lyr_name)
    geom = ogr.Geometry(ogr.wkbGeometryCollection)

    for feat in lyr:
       geom.AddGeometry(feat.GetGeometryRef())

    # create a convex hull
    geom = geom.GetEnvelope()
    wktBounds = 'POLYGON (({} {}, {} {}, {} {}, {} {}, {} {}, {} {}))'.format(geom[1], geom[3], geom[0], geom[3],
                                                                       geom[0], geom[2], geom[1], geom[2],
                                                                       geom[1], geom[3], geom[1], geom[3])
    return wktBounds


def wktFromKML(kmlfile):

    shp = ogr.Open(os.path.abspath(kmlfile))
    lyr = shp.GetLayerByName()
    for feat in lyr:
       geom = feat.GetGeometryRef()
    wkt = str(geom)

    return wkt


def reprojectGeom(geom, inProj4, outEPSG):
    """
    Reproject a wkt geometry based on EPSG code
    """

    geom = ogr.CreateGeometryFromWkt(geom)
    # input SpatialReference
    inSpatialRef = osr.SpatialReference()
    inSpatialRef.ImportFromProj4(inProj4)

    # output SpatialReference
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(int(outEPSG))

    # create the CoordinateTransformation
    coordTrans = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)
    geom.Transform(coordTrans)
    return geom


def aoiWKT(shpfile):
    """
    Prepare a wkt geometry for the S1 search routine
    """

    # get the shapefile, projection file and epsg
    shpfile = os.path.abspath(shpfile)
    prjfile = shpfile[:-4] + '.prj'
    proj4 = getProj4(prjfile)
    wkt = wktExtentFromShapefile(shpfile)

    if proj4 != '+proj=longlat +datum=WGS84 +no_defs':
        print(' INFO: Reprojecting AOI file to Lat/Long (WGS84)')
        wkt = reprojectGeom(wkt, proj4, 4326)

    return wkt


def llPoint2shp(lon, lat, shpFile):
    
    schema = {'geometry': 'Point',
              'properties': {'id': 'str'}
             }
    
    wkt = loads('POINT ({} {})'.format(lon, lat))
    
    with collection(shpFile, "w", crs=from_epsg(4326), driver="ESRI Shapefile", schema=schema) as output:
        output.write({'geometry': mapping(wkt),
                'properties': {'id': '1'}                      
                })
        
        
def aoi2Gdf(aoi):
    
    # load AOI as GDF
    if aoi.split('.')[-1] == 'shp':
        
        gdfAoi = gpd.GeoDataFrame.from_file(aoi)
        
        if gdfAoi.geom_type.values[0] is not 'Polygon':
            print(' ERROR: aoi file needs to be a polygon shapefile')
        #    sys.exit()
            
        prjfile = aoi[:-4] + '.prj'
        proj4 = getProj4(prjfile)
        
        if proj4 != '+proj=longlat +datum=WGS84 +no_defs':
            print(' INFO: reprojecting AOI layer to WGS84.')
            # reproject
            gdfAoi.crs = (proj4)
            gdfAoi = gdfAoi.to_crs({'init': 'epsg:4326'})
            
    else:
        # load a world_file
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        gdfAoi = world[world['iso_a3'] == aoi]
    
    # and set the crs (hardcoded!!!)
    gdfAoi.crs = fiona.crs.from_epsg(4326) 
    
    return gdfAoi


def gdfInv2Shp(fpDataFrame, outFile):

    # change datetime datatypes
    fpDataFrame['acquisitiondate'] = fpDataFrame['acquisitiondate'].astype(str)
    fpDataFrame['ingestiondate'] = fpDataFrame['ingestiondate'].astype(str)
    fpDataFrame['beginposition'] = fpDataFrame['beginposition'].astype(str)
    fpDataFrame['endposition'] = fpDataFrame['endposition'].astype(str)
    
    # write to shapefile
    fpDataFrame.to_file(outFile)
    

def plotInv(aoi, footprintGdf, transperancy=0.05):
    
    import matplotlib.pyplot as plt 

    # load world borders for background
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

    # import aoi as gdf
    gdfAoi = aoi2Gdf(aoi)
    
    # get bounds of AOI
    bounds = footprintGdf.geometry.bounds

    # get world map as base
    base = world.plot(color='lightgrey', edgecolor='white')

    # plot aoi 
    gdfAoi.plot(ax=base, color='None', edgecolor='black')

    # plot footprints
    footprintGdf.plot(ax=base, alpha=transperancy)

    # set bounds
    plt.xlim([bounds.minx.min()-2, bounds.maxx.max()+2])
    plt.ylim([bounds.miny.min()-2, bounds.maxy.max()+2])
    plt.grid(color='grey', linestyle='-', linewidth=0.2)
