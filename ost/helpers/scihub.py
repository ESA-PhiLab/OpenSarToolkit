import getpass
import datetime
import urllib
import geopandas as gpd

from ost.helpers import vector as vec

# -*- coding: utf-8 -*-
def askScihubCreds():
    
    # SciHub account details (will be asked by execution)
    print(' If you do not have a Copernicus Scihub user account'
          ' go to: https://scihub.copernicus.eu and register')
    uname = input(' Your Copernicus Scihub Username:')
    pword = getpass.getpass(' Your Copernicus Scihub Password:')
    
    return uname, pword


def scihubConnect(baseURL, uname, pword):
    """
    Connect and authenticate to the scihub server.
    """

    # open a connection to the scihub
    manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    manager.add_password(None, baseURL, uname, pword)
    handler = urllib.request.HTTPBasicAuthHandler(manager)
    opener = urllib.request.build_opener(handler)

    return opener


def nextPage(dom):
    """
    Use this function to iterate over the search results
    due to the limit of a maximum of 100 results per query.
    """

    links = dom.getElementsByTagName('link')
    next, self, last = None, None, None

    for link in links:
        if link.getAttribute('rel') == 'next':
            next = link.getAttribute('href')
        elif link.getAttribute('rel') == 'self':
            self = link.getAttribute('href')
        elif link.getAttribute('rel') == 'last':
            last = link.getAttribute('href')

    if last == self:     # we are at the end
        return None
    else:
        return next


def createSatStr(sat):
    
    if str(1) in sat:
        sat = 'Sentinel-1'
    elif str(2) in sat:
        sat = 'Sentinel-2'
    elif str(3) in sat:
        sat = 'Sentinel-3'
    elif str(5) in sat:
        sat = 'Sentinel-5'
        
    return 'platformname:{}'.format(sat)


def createAoiWkt(aoi='*'):
    
    # bring aoi to query format
    if aoi is not '*':
        if aoi.split('.')[-1] != 'shp':
            print('get wkt country boundaries from geopandas low res data')
            world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
            geom = world['geometry'][world['iso_a3'] == aoi].tolist()[0].bounds
            aoi = 'POLYGON (({} {}, {} {}, {} {}, \
                   {} {}, {} {}))'.format(geom[0], geom[1], geom[2], geom[1],
                                          geom[2], geom[3], geom[0], geom[3],
                                          geom[0], geom[1])

            if len(aoi) == 0:
                print(' No country found for this ISO code')
                exit(1)

            aoi = "( footprint:\"Intersects({})\")".format(aoi)

        else:
            aoi = vec.aoiWKT(aoi)
            aoi = "( footprint:\"Intersects({})\")".format(aoi)

    return aoi


def createToiStr(startDate='2014-10-01', 
                 endDate=datetime.datetime.now().strftime("%Y-%m-%d")):

    # bring start and end date to query format
    startDate = '{}T00:00:00.000Z'.format(startDate)
    endDate = '{}T23:59:59.999Z'.format(endDate)
    toi = ('beginPosition:[{} TO {}] AND '
           'endPosition:[{} TO {}]'.format(startDate, endDate, 
                                           startDate, endDate))
     
    return toi


def createS1ProdSpecs(pType='*', polMode='*', beam='*'):
    
    # bring start and end date to query format
    # bring product type, polMode and beam to query format
    pType = "producttype:{}".format(pType)
    polMode = "polarisationMode:{}".format(polMode)
    beam = "sensoroperationalmode:{}".format(beam)
    
    return '{} AND {} AND {}'.format(pType, polMode, beam)


def createQuery(satellite, aoi, toi, prodSpecs):
    """
    Create the query part of the url compatible with Copernicus Scihub.
    """
    # construct the final query
    query = urllib.request.quote('{} AND {} AND {} AND \
                                  {}'.format(satellite, prodSpecs, aoi, toi))
    return query