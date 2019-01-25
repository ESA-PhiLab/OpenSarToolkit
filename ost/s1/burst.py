# -*- coding: utf-8 -*-
import os
import zipfile
import geopandas as gpd

from ost.helpers import vector as vec
from ost.s1 import metadata


def createBurstGdf(footprintGdf, uname=None, pword=None):
    
    # create column names for empty data frame
    colNames = ['SceneID', 'Date', 'SwathID', 'BurstID',
                'BurstNr', 'geometry']
    
    # crs for empty dataframe
    crs = {'init': 'epsg:4326'}
    # create empty dataframe
    gdfFull = gpd.GeoDataFrame(columns=colNames, crs=crs)
    #uname, pword = scihub.askScihubCreds()
    
    for sceneId in footprintGdf.identifier:
        #print(metadata.s1Metadata(sceneId).s1IPTpath())
        s = metadata.s1Metadata(sceneId)
        if os.path.exists(s.s1IPTpath()):
            #print('here')
            gdfFull = gdfFull.append(s.s1IPTAnno())
        else:
            if s.checkOnlineStatus is False:
                print(' INFO: Product needs to be online to create a burst database.')
                print(' INFO: Download the product first and do the burst list from the local data.')
            else:
                gdfFull = gdfFull.append(s.s1EsaAnno(uname, pword))
            
    return gdfFull


def createBurstGdfOffline(footprintGdf, dwnDir):
    
    # create column names for empty data frame
    colNames = ['SceneID', 'Date', 'SwathID', 'BurstID',
                'BurstNr', 'geometry']
    
    # crs for empty dataframe
    crs = {'init': 'epsg:4326'}
    # create empty dataframe
    gdfFull = gpd.GeoDataFrame(columns=colNames, crs=crs)
    
    for sceneId in footprintGdf.identifier:
        
        s = metadata.s1Metadata(sceneId)
        gdfFull = gdfFull.append(s.s1DwnAnno(dwnDir))
        
    return gdfFull


def createBurstGdfIpt(footprintGdf):
    
    # create column names for empty data frame
    colNames = ['SceneID', 'Date', 'SwathID', 'BurstID',
                'BurstNr', 'geometry']
    
    # crs for empty dataframe
    crs = {'init': 'epsg:4326'}
    # create empty dataframe
    gdfFull = gpd.GeoDataFrame(columns=colNames, crs=crs)
    
    for sceneId in footprintGdf.identifier:
        print(sceneId)
        s = metadata.s1Metadata(sceneId)
        print(s.s1IPTAnno())
        gdfFull = gdfFull.append(s.s1IPTAnno())
        
    return gdfFull


def refineBurstGdf(aoi, burstGdf):
    
    # turn aoi into a geodataframe
    gdfAoi = vec.aoi2Gdf(aoi)
    
    # get columns of input dataframe for later return function
    cols = burstGdf.columns
   
    # 1) get only intersecting footprints (double, since we do this before)
    burstGdf = gpd.sjoin(burstGdf, gdfAoi, how='inner',op='intersects')
    
    # if aoi  gdf has an id field we need to rename the changed id_left field
    if 'id_left' in burstGdf.columns.tolist():
        #rename id_left to id
        burstGdf.columns = ['id' if x == 'id_left' else x for x in burstGdf.columns.tolist()]
        
    return burstGdf[cols]