# -*- coding: utf-8 -*-
import os
import geopandas as gpd

from ost.helpers import vector as vec
from ost.s1 import metadata


def createBurstGdf(footprintGdf, dwnDir=os.getenv('HOME'), uname=None, pword=None):

    # create column names for empty data frame
    colNames = ['SceneID', 'Track', 'Date', 'SwathID', 'AnxTime',
                'BurstNr', 'geometry']

    # crs for empty dataframe
    crs = {'init': 'epsg:4326'}
    # create empty dataframe
    gdfFull = gpd.GeoDataFrame(columns=colNames, crs=crs)
    #uname, pword = scihub.askScihubCreds()

    for sceneId in footprintGdf.identifier:
        #print(metadata.s1Metadata(sceneId).s1IPTpath())
        s = metadata.s1Metadata(sceneId)
        if os.path.exists(s.s1DwnPath(dwnDir)):
            print(' Getting burst info from downloaded files')
            gdfFull = gdfFull.append(s.s1DwnAnno(dwnDir))
        elif os.path.exists(s.s1CreoPath()):
            print(' Getting burst info from Creodias eodata store')
            gdfFull = gdfFull.append(s.s1CreoAnno())
        else:
            print(' Getting burst info from scihub (need to download xml files)')
            if s.checkOnlineStatus is False:
                print(' INFO: Product needs to be online to create a burst database.')
                print(' INFO: Download the product first and do the burst list from the local data.')
            else:
                gdfFull = gdfFull.append(s.s1EsaAnno(uname, pword))

    gdfFull = gdfFull.reset_index(drop=True)
    
    for i in gdfFull['AnxTime'].unique():
    
        # get similar burst times
        idx = gdfFull.index[(gdfFull.AnxTime >= i - 1) & 
                            (gdfFull.AnxTime <= i + 1) & 
                            (gdfFull.AnxTime != i)].unique().values

        # reset all to first value
        for j in idx:
            gdfFull.at[j, 'AnxTime'] = i

    gdfFull['bid'] = 'T' + gdfFull.Track.astype(str) + '_' + \
                  gdfFull.SwathID.astype(str) + '_' + \
                  gdfFull.AnxTime.astype(str)
            
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

    gdfFull.BurstID = gdfFull.BurstID.map(lambda x: str(x)[:-1])
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
        #print(sceneId)
        s = metadata.s1Metadata(sceneId)
        #print(s.s1CreoAnno())
        gdfFull = gdfFull.append(s.s1CreoAnno())

    gdfFull.BurstID = gdfFull.BurstID.map(lambda x: str(x)[:-1])
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
