#! /usr/bin/env python
"""
This script allows to sort Sentinel-1 data for homogeneous large-scale mapping.
"""

# import stdlib modules
import os
import itertools 

# some more libs for plotting and DB connection
import fiona
import pandas as pd
import geopandas as gpd

from shapely.ops import unary_union

# import internal modules
from ost.helpers.db import pgHandler
from ost.helpers import vector as vec
from ost.s1.metadata import s1Metadata


# script infos
__author__ = 'Andreas Vollrath'
__copyright__ = 'phi-lab, European Space Agency'

__license__ = 'GPL'
__version__ = '1.0'
__maintainer__ = 'Andreas Vollrath'
__email__ = ''
__status__ = 'Production'


#--------------------------------------------------------------------
def readS1Inventory(inputData):
    '''
    This function intends to transform different spatial formats in which inventory can be stored
    to transform into a geopandas GeoDataFrame that will be handled by all other methods.
    
    param: inputData
    returns: GeoDataFrame
    '''
        
    if inputData[-4:] == '.shp':
        print(' INFO: Importing Sentinel-1 inventory data from ESRI shapefile:\n {}'.format(inputData))
        colNames = ['id','identifier', 'polarisationmode', 'orbitdirection', 
                'acquisitiondate', 'relativeorbit', 'orbitnumber', 
                'producttype','slicenumber', 'size', 'beginposition', 
                'endposition', 'lastrelativeorbitnumber', 'lastorbitnumber',
                'uuid', 'platformidentifier', 'missiondatatakeid',
                'swathidentifier', 'ingestiondate','sensoroperationalmode',
                'geometry']
        
        outFrame = gpd.read_file(inputData)
        outFrame.columns = colNames
        
    elif inputData[-7:] == '.sqlite':
        print(' INFO: Importing Sentinel-1 inventory data from spatialite DB file:\n {}'.format(inputData))
        # needs to be added
    else:
        print(' INFO: Importing Sentinel-1 inventory data from PostGreSQL DB table:\n {}'.format(inputData))
        dbConnect = pgHandler()
        sql = 'select * from {}'.format(inputData)
        outFrame = gpd.GeoDataFrame.from_postgis(sql, dbConnect.connection, geom_col='geometry')
    
    if len(outFrame) >= 0:
        print(' INFO: Succesfully converted inventory data into a GeoPandas Geo-Dataframe.')
    
    return outFrame


def removeDoubleEntries(footprintGdf):
    
    # filter footprint data frame for obit direction and polarisation & get unqiue entries 
    idx = footprintGdf.groupby(footprintGdf['identifier'].str.slice(0,63))['ingestiondate'].transform(max) == footprintGdf['ingestiondate']
    
    # re-initialize GDF geometry due to groupby function
    crs = fiona.crs.from_epsg(4326)  
    gdf = gpd.GeoDataFrame(footprintGdf[idx], geometry='geometry', crs=crs)
    print(' INFO: {} frames remain after double entry removal'.format(len(footprintGdf[idx])))
    return gdf
    

def removeNonAoiOverlap(gdfAoi, footprintGdf):
    '''
    '''
    
    # get columns of input dataframe for later return function
    cols = footprintGdf.columns
   
    # 1) get only intersecting footprints (double, since we do this before)
    footprintGdf = gpd.sjoin(footprintGdf, gdfAoi, how='inner',op='intersects')
    
    # if aoi  gdf has an id field we need to rename the changed id_left field
    if 'id_left' in footprintGdf.columns.tolist():
        #rename id_left to id
        footprintGdf.columns = ['id' if x == 'id_left' else x for x in footprintGdf.columns.tolist()]
        
    return footprintGdf[cols]


def restructureEquatorCrossing(footprintGdf):

    #get the relativeorbitnumbers that change with equator crossing
    tracks = footprintGdf.lastrelativeorbitnumber[footprintGdf['relativeorbit'] != footprintGdf['lastrelativeorbitnumber']].unique().tolist()

    for track in tracks:
        
        # get dates 
        dates = footprintGdf.acquisitiondate[(footprintGdf['relativeorbit'] == track)].unique()
        for date in dates:
#
#            #----------------------------------------------------
#            #### NEEDS TO BE ADDED THE CHECK
#            # check if consecutive orbitnumers are from the same track
#            subdf = footprintGdf[(footprintGdf['acquisitiondate'] == date) &
#                                 (footprintGdf['relativeorbit'] == track) |
#                                 (footprintGdf['acquisitiondate'] == date) &
#                                 (footprintGdf['relativeorbit'] == 
#                                  str(int(track) - 1))].sort_values(['beginposition'])
#            
#            for row in subdf.iterrows():
#            #----------------------------------------------------
            
            # get index of 
            idx = footprintGdf[(footprintGdf['acquisitiondate'] == date) &
                               (footprintGdf['relativeorbit'] == track)].index

            # reset relative orbit number
            footprintGdf.set_value(idx, 'relativeorbit', str(int(track) - 1))

    return footprintGdf

    
def removeNonAoiTracks(gdfAoi, fpDataFrame, areaReduce=0.1):
    '''
    This function takes the AOI and the footprint inventory
    and checks if any of the tracks are unnecessary, i.e. 
    if the AOI can be covered by all the remaining tracks.
    
    The output will be a subset of the given dataframe, 
    including only the relevant tracks.
    '''
      
    # get Area of AOI
    aoiArea = gdfAoi.area.sum()
    
    # create a list of tracks for that date (sometimes more than one)    
    trackList = fpDataFrame['relativeorbit'].unique()
    
    for track in trackList:
        
        trackUnion = fpDataFrame.geometry[fpDataFrame['relativeorbit'] != track].unary_union
        interTrack = gdfAoi.geometry.intersection(trackUnion).area.sum()

        if interTrack >= aoiArea - areaReduce:
            print(' INFO: excluding track {}'.format(track))
            fpDataFrame = fpDataFrame[fpDataFrame['relativeorbit'] != track]
    
    print(' INFO: {} frames remain after non-AOI overlap'.format(len(fpDataFrame)))
    return fpDataFrame


def removeNonFullSwath(gdfAoi, footprintGdf):
    
    # define final output gdf
    outFrame = gpd.GeoDataFrame(columns = footprintGdf.columns)
    
    # create a list of tracks for that date (sometimes more than one)    
    trackList = footprintGdf['relativeorbit'].unique()
    
    for track in trackList:
        
        # get area of AOI intersect for all acq.s of this track
        trackUnion = footprintGdf['geometry'][footprintGdf['relativeorbit'] == track].unary_union
        interTrack = gdfAoi.geometry.intersection(trackUnion).area.sum()
       
        # loop through dates
        for date in sorted(footprintGdf['acquisitiondate'][footprintGdf['relativeorbit'] == track].unique(), reverse=False):
        
            gdfDate = footprintGdf[(footprintGdf['relativeorbit'] == track) &
                          (footprintGdf['acquisitiondate'] == date)]
            
            # get area of AOI intersect for all acq.s of this track
            dateUnion = gdfDate.geometry.unary_union
            interDate = gdfAoi.geometry.intersection(dateUnion).area.sum()

    
            if interTrack <= interDate + 0.15:
                outFrame = outFrame.append(gdfDate)          
    
    print(' INFO: {} frames remain after removal of non-full AOI crossing'.format(len(outFrame)))
    return outFrame


def checkNonContinousSwath(footprintGdf):

    tracks = footprintGdf.lastrelativeorbitnumber.unique()
    footprintGdf['slicenumber'] = footprintGdf['slicenumber'].astype(int)

    for track in tracks:

        dates = footprintGdf.acquisitiondate[footprintGdf['relativeorbit'] == track].unique()

        for date in dates:

            subdf = footprintGdf[(footprintGdf['acquisitiondate'] == date) &
                                 (footprintGdf['relativeorbit'] == track)].sort_values('slicenumber')
            
            if len(subdf) <= int(subdf.slicenumber.max()) - int(subdf.slicenumber.min()):

                i = 1
                lastSlice = int(subdf.slicenumber.min()) - 1
                
                for index, row in subdf.iterrows():

                    if int(row.slicenumber) - int(lastSlice) > 1:    
                        i += 1

                    uuid = row.uuid
                    newId = '{}.{}'.format(row.relativeorbit, i)
                    idx = footprintGdf[footprintGdf['uuid'] == uuid].index
                    footprintGdf.set_value(idx, 'relativeorbit', newId)
                    lastSlice = row.slicenumber
            
    return footprintGdf


def forwardCoverage(gdfAOI, fpDataFrame, areaReduce=0):
    '''
    This functions loops through the acquisition dates and 
    identifies the time interval needed to create full coverages.
    '''
    
    
    # get AOI area 
    aoiArea = gdfAOI.area.sum()
    
    # initialize some stuff for subsequent for-loop
    intArea, i = 0, 0
    dateList = []
    gdfUnion = None
    dateStart = None
    outFrame = gpd.GeoDataFrame(columns = fpDataFrame.columns)
    
    # loop through dates
    for date in sorted(fpDataFrame['acquisitiondate'].unique(), reverse=False):
    
        # set starting date for curent mosaic
        if dateStart is None:
            dateStart = date
        
            # ofr th emoment, just take the first mosaic
            if i != 0:
                break
            
        # create a list of tracks for that date (sometimes more than one)    
        trackList = fpDataFrame['relativeorbit'][(fpDataFrame['acquisitiondate'] == date)].unique()
        
        for track in trackList:
            
            # get all footprints for each date
            gdf = fpDataFrame[(fpDataFrame['acquisitiondate'] == date) &
                              (fpDataFrame['relativeorbit'] == track)]
        
            # get a unified geometry for date/track combination
            geomUnion = gdf.geometry.unary_union
        
            # add to overall union and to outFrame
            outFrame = outFrame.append(gdf)

            # just for first loop
            if gdfUnion is None:
                # create new overall union
                gdfUnion = geomUnion
            else:
                # union of unified footprints and footprints before
                polys = [gdfUnion, geomUnion]
                gdfUnion = unary_union(polys) 

            # get intersection with aoi and calculate area
            inter = gdfAOI.geometry.intersection(gdfUnion)
            intArea = inter.area.sum()
            
            # for the dateList, we reset some stuff for next mosaic                                   
            if intArea >= aoiArea - areaReduce:
                dateList.append([dateStart, date])
                dateStart = None
                gdfUnion = None
                
        #i += 1

    return dateList, gpd.GeoDataFrame(outFrame, geometry='geometry')


def backwardCoverage(gdfAOI, fpDataFrame, dateList, areaReduce=0):
    '''
    This function takes the footprint dataframe and the datelist
    created by the forwardCoverage function to sort out
    duplicate tracks apparent in the mosaics.
    It searches from the last acqusiition date backwards
    in order to assure minimum time gap between the acquisitions of
    different swaths.
    '''
    
    import geopandas as gpd
    from shapely.ops import unary_union
    # get AOI area
    aoiArea = gdfAOI.area.sum()
    
    # create empty dataFrame for output
    tmpFrame = gpd.GeoDataFrame(columns = fpDataFrame.columns)
    outFrame = gpd.GeoDataFrame(columns = fpDataFrame.columns)
    gdfUnion, intArea = None, 0
    
    # sort the single full coverages from forwardCoverage
    for dates in dateList:
        
        #print(dates[0], dates[1])
        
        # extract scenes for single mosaics
        gdf = fpDataFrame[(fpDataFrame['acquisitiondate'] <= dates[1]) &
                          (fpDataFrame['acquisitiondate'] >= dates[0])]
       
        # we create an emtpy list and fill with tracks used for the mosaic, so they are not used twice
        tracksIncluded = []
        
        # loop through dates backwards
        for date in sorted(gdf['acquisitiondate'].unique(), reverse=True):
            
            # create a list of tracks for that date (sometimes more than one)    
            trackList = gdf['relativeorbit'][(gdf['acquisitiondate'] == date)].unique()
   
            for track in trackList:

                 # we want every track just once, so we check 
                if track not in tracksIncluded:
                    
                    tracksIncluded.append(track)
                    
                    # get all footprints for each date and track
                    gdfTrack = gdf[(gdf['acquisitiondate'] == date) &
                                   (gdf['relativeorbit'] == track)]

                    # re-initialize GDF due to groupby fucntion
                    gdfTrack = gpd.GeoDataFrame(gdfTrack, geometry='geometry')

                    # get a unified geometry for date/track combination
                    geomUnion = gdfTrack.geometry.unary_union

                    # add to overall union and to outFrame
                    tmpFrame = tmpFrame.append(gdfTrack)

                    # just for first loop
                    if gdfUnion is None:
                        # create new overall union
                        gdfUnion = geomUnion
                    else:
                        # union of unified footprints and footprints before
                        polys = [gdfUnion, geomUnion]
                        gdfUnion = unary_union(polys) 

                    # get intersection with aoi and calulate area
                    inter = gdfAOI.geometry.intersection(gdfUnion)
                    intArea = inter.area.sum()

                    # we break the loop if we found enough                              
                    if intArea >= aoiArea - areaReduce:

                        # cleanup scenes 
                        #tmpFrame = gpd.GeoDataFrame(tmpFrame, geometry='geometry')
                        outFrame = outFrame.append(tmpFrame)
                        tmpFrame = gpd.GeoDataFrame(columns = fpDataFrame.columns)
                        gdfUnion = None
                        # stop for loop
                        break

            # we break the loop if we found enough                                
            if intArea >= aoiArea - areaReduce:
                break

    return gpd.GeoDataFrame(outFrame, geometry='geometry', crs={'init': 'epsg:4326'})


def searchRefinement(aoi, footprintGdf, invDir,
                     marginalTracks=False, 
                     fullCrossAoi=True, 
                     mosaicRefine=True, 
                     areaReduce=0.05):

    # creat AOI GeoDataframe and calulate area
    gdfAoi = vec.aoi2Gdf(aoi)
    aoiArea = gdfAoi.area.sum()
    # get all polarisations apparent in the inventory
    pols = footprintGdf['polarisationmode'].unique()
    
    # get orbit directions apparent in the inventory
    orbitDirs = footprintGdf['orbitdirection'].unique()
    
    # create inventoryDict
    invDict = {}
    covDict = {}
    
    # loop through all possible combinations
    for pol, orb in itertools.product(pols, orbitDirs):
    #for orb in orbitDirs: 

        print(' INFO: Coverage analysis for {} tracks in {} polarisation.'.format(orb, pol))
        # subset the footprint for orbit direction
        #footprintGdfSort = footprintGdf[footprintGdf['orbitdirection'] == orb]
        footprintGdfSort = footprintGdf[(footprintGdf['polarisationmode'] == pol) &
                                        (footprintGdf['orbitdirection'] == orb)]

        print(' INFO: {} frames for {} tracks in {} polarisation.'.format(len(footprintGdfSort), orb, pol))
        # calculate intersected area
        inter = gdfAoi.geometry.intersection(footprintGdfSort.unary_union)
        intArea = inter.area.sum()

        if intArea <= aoiArea - areaReduce:
            print(' WARNING: Set of footprints does not fully cover AOI. ')

        else:

            # apply the different sorting steps
            footprintsRef = removeDoubleEntries(footprintGdfSort)
            
            footprintsRef = removeNonAoiOverlap(gdfAoi, footprintsRef)

            if orb == 'ASCENDING':
                footprintsRef = restructureEquatorCrossing(footprintsRef)

            if marginalTracks is False:
                footprintsRef = removeNonAoiTracks(gdfAoi, footprintsRef, areaReduce)

            if fullCrossAoi is True:
                footprintsRef = removeNonFullSwath(gdfAoi, footprintsRef)

            footprintsRef = checkNonContinousSwath(footprintsRef)

            if mosaicRefine is True:
                dateList, footprintsRef  = forwardCoverage(gdfAoi, footprintsRef, areaReduce)
                footprintsRef = backwardCoverage(gdfAoi, footprintsRef, dateList,areaReduce)

            if footprintsRef is not None:
                vec.gdfInv2Shp(footprintsRef, '{}/{}_{}_{}.shp'.format(invDir, len(dateList), orb, ''.join(pol.split())))
                invDict['{}_{}'.format(orb, ''.join(pol.split()))] = footprintsRef
                covDict['{}_{}'.format(orb, ''.join(pol.split()))] = len(dateList)
                
            print(' INFO: Found {} full coverage mosaics for {}.'.format(len(dateList), aoi))
            
    return invDict, covDict


def createProcDict(inputFrame):
    
    # initialize empty dictionary
    dictScenes = {}
    
    # get relative orbits and loop through each
    trackList = inputFrame['relativeorbit'].unique()
    for track in trackList:
        
        # initialize an empty list that will be filled by list of scenes per acq. date
        allIDs = []
        
        # get acquisition dates and loop through each
        acqdates = inputFrame['acquisitiondate'][inputFrame['relativeorbit'] == track].unique()
        for acqdate in acqdates:
                                
            # get the scene ids per acqdate and write into a list
            singleIDs=[]
            singleIDs.append(inputFrame['identifier'][(inputFrame['relativeorbit'] == track) &
                                                      (inputFrame['acquisitiondate'] == acqdate)].tolist())
            
            # append the list of scenes to the list of scenes per track
            allIDs.append(singleIDs[0])
            
        # add this list to the dctionary and associate the track number as dict key
        dictScenes[track] = allIDs
        
    return dictScenes