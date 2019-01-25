# -*- coding: utf-8 -*-
# import stdlib modules
import os
import time
import zipfile
import getpass
import requests
import multiprocessing

# import external modules
import tqdm
import pandas as pd

from ost.s1.metadata import s1Metadata

# script infos
__author__ = 'Andreas Vollrath'
__copyright__ = 'phi-lab, European Space Agency'

__license__ = 'GPL'
__version__ = '1.0'
__maintainer__ = 'Andreas Vollrath'
__email__ = ''
__status__ = 'Production'


def checkSceneAvailability(inputGDF, dwnDir, cloudProvider=None):
    '''
    This function checks for the availability of scenes inside a geodataframe
    on different cloud providers and flags the ones that need to be downloaded.
    
    Note: Should be applied after readInventory and before download
    
    param: inputGDF is a GeoDataFrame coming from a search and possible pre-sorting
    param: downDir is the directory where scenes  should be downloaded
    param: cloudProvider defines on which cloud we operate (IPT, AWS, OTC)
    
    returns: a GeoDataFrame with all scenes and a flag of which scenes need to be downloaded
    '''
    
    print(' INFO: Checking if scenes need to be downloaded.')
    # create an empty DataFrame
    df = pd.DataFrame(columns=['identifier', 'filepath', 'toDownload']) 
    
    # loop through each scene
    scenes = inputGDF['identifier'].tolist()
    for sceneID in scenes:

        scene = s1Metadata(sceneID)
        
        # check if we can download from the cloudprovider
        if cloudProvider == 'IPT':
            testPath = scene.s1IPTpath()
        elif cloudProvider == 'Amazon':
            testPath = scene.s1AmazonPath() # function needs to be added
        elif cloudProvider == 'T-Cloud':
            testPath = scene.s1TCloudPath() # function needs to be added
        else:
            # construct download path
            testPath = '{}/SAR/{}/{}/{}/{}/{}.zip'.format(dwnDir, scene.product_type,
                                                          scene.year, scene.month, scene.day, sceneID)
           
        # if the file exists!
        ### NOTE THAT at the moment we assume IPT structure where files are stored in SAFE format, 
        ### i.e. they are directories, not zips 
       
        if os.path.isdir(testPath) is True or os.path.exists(testPath) is True:
            
            # if we are not in cloud
            if dwnDir in testPath:
                
                # file is already succesfully downloaded
                df = df.append({'identifier': sceneID,
                                'filepath': testPath, 
                                'toDownload' : False}, ignore_index=True)
            else:
                
                # file is on cloud storage 
                df = df.append({'identifier': sceneID,
                                'filepath': testPath, 
                                'toDownload' : False}, ignore_index=True)
        
        else:
            
            # construct download path to check if we already downloaded
            testPath = '{}/SAR/{}/{}/{}/{}/{}.zip'.format(dwnDir, scene.product_type,
                                                          scene.year, scene.month, scene.day, sceneID)
           
            # if we are on cloud, check if we already downloaded
            if os.path.exists(testPath) is True:
                
                # file is already succesfully downloaded
                df = df.append({'identifier': sceneID,
                                'filepath': testPath, 
                                'toDownload' : False}, ignore_index=True)
            
            else:
                df = df.append({'identifier': sceneID,
                                'filepath': testPath, 
                                'toDownload' : True}, ignore_index=True) 
    
    # merge the dataframe and return it
    inputGDF = inputGDF.merge(df, on = 'identifier')
    return inputGDF



def checkApihubConn(uname, pword):

    url = ('https://scihub.copernicus.eu/apihub/odata/v1/Products?' 
          '$select=Id&$filter=substringof(%27_20171113T010515_%27,Name)')
    response = requests.get(url, auth=(uname, pword))
    return response.status_code


def s1ApihubDownload(argumentList, tries=50):

    """
    This function will download S1 products from ESA's apihub.

    :param url: the url to the file you want to download
    :param fileName: the absolute path to where the 
                     downloaded file should be written to
    :param uname: ESA's scihub username
    :param pword: ESA's scihub password
    :return:
    """
    uuid = argumentList[0]
    fileName = argumentList[1]
    uname = argumentList[2]
    pword = argumentList[3]

    # ask for username and password in case you have not defined as input
    if uname == None:
        print(' If you do not have a Copernicus Scihub user'
              ' account go to: https://scihub.copernicus.eu')
        uname = input(' Your Copernicus Scihub Username:')
    if pword == None:
        pword = getpass.getpass(' Your Copernicus Scihub Password:')

    # define url
    url = ('https://scihub.copernicus.eu/apihub/odata/v1/'
           'Products(\'{}\')/$value'.format(uuid))

    # get first response for file Size
    response = requests.get(url, stream=True, auth=(uname, pword))

    # check response
    if response.status_code == 401:
        raise ValueError(' ERROR: Username/Password are incorrect.')
    elif response.status_code != 200:
        print(' ERROR: Something went wrong, will try again in 30 seconds.')
        response.raise_for_status()

    # get download size
    totalLength = int(response.headers.get('content-length', 0))

    # define chunksize
    chunkSize = 1024

    # check if file is partially downloaded
    if os.path.exists(fileName):
        firstByte = os.path.getsize(fileName)
    else:
        firstByte = 0

    if firstByte >= totalLength:
        return totalLength

    while firstByte < totalLength:

        # get byte offset for already downloaded file
        header = {"Range": "bytes={}-{}".format(firstByte, totalLength)}

        print(' INFO: Downloading scene to: {}'.format(fileName))
        response = requests.get(url, headers=header, stream=True,
                                auth=(uname, pword))

        # actual download
        with open(fileName, "ab") as f:

            if totalLength is None:
                f.write(response.content)
            else:
                pbar = tqdm.tqdm(total=totalLength, initial=firstByte, 
                                 unit='B', unit_scale=True, 
                                 desc=' INFO: Downloading: ')
                for chunk in response.iter_content(chunkSize):
                    if chunk:
                        f.write(chunk)
                        pbar.update(chunkSize)
        pbar.close()
        # update firstByte
        firstByte = os.path.getsize(fileName)


def checkPepsConn(uname, pword):

    response = requests.get('https://peps.cnes.fr/rocket/#/search?view=list&maxRecords=50', auth=(uname, pword))
    return response.status_code


def s1PepsDownload(argumentList):
    """
    This function will download S1 products from CNES Peps mirror.

    :param url: the url to the file you want to download
    :param fileName: the absolute path to where the downloaded file should be written to
    :param uname: CNES Peps username
    :param pword: CNES Peps password
    :return:
    """

    url = argumentList[0]
    fileName = argumentList[1]
    uname = argumentList[2]
    pword = argumentList[3]

    downloaded = False
    
    while downloaded is False:
        
        # get first response for file Size
        response = requests.get(url, stream=True, auth=(uname, pword))
    
        # get download size
        totalLength = int(response.headers.get('content-length', 0))
    
        # define chunksize
        chunkSize = 1024
    
        # check if file is partially downloaded
        if os.path.exists(fileName):
            
            firstByte = os.path.getsize(fileName)
            if firstByte == totalLength:
                print(' INFO: {} already downloaded.'.format(fileName))
            else:
                print(' INFO: Continue downloading scene to: {}'.format(fileName))
                
        else:
            print(' INFO: Downloading scene to: {}'.format(fileName))
            firstByte = 0
    
        if firstByte >= totalLength:
            return totalLength
    
        # get byte offset for already downloaded file
        header = {"Range": "bytes={}-{}".format(firstByte, totalLength)}
        response = requests.get(url, headers=header, stream=True, auth=(uname, pword))
    
        # actual download
        with open(fileName, "ab") as f:
    
            if totalLength is None:
                f.write(response.content)
            else:
                pbar = tqdm.tqdm(total=totalLength, initial=firstByte, unit='B',
                                unit_scale=True, desc=' INFO: Downloading: ')
                for chunk in response.iter_content(chunkSize):
                    if chunk:
                        f.write(chunk)
                        pbar.update(chunkSize)
        pbar.close()
    
        # zipFile check
        print(' INFO: Checking the zip archive of {} for inconsistency'.format(fileName))
        zipArchive = zipfile.ZipFile(fileName)
        zipTest = zipArchive.testzip()
    
        # if it did not pass the test, remove the file 
        # in the while loop it will be downlaoded again
        if zipTest is not None:
            print(' INFO: {} did not pass the zip test. Re-downloading the full scene.'.format(fileName))
            os.remove(fileName)
        # otherwise we change the status to True
        else:
            print(' INFO: {} passed the zip test.'.format(fileName))
            downloaded = True
        

def batchDownloadPeps(fpDataFrame, dwnDir, uname, pword, concurrent=10):

        print(' INFO: Getting the storage status (online/onTape) of each scene on the Peps server.')
        print(' INFO: This may take a while.')
        
        # this function does not just check, 
        # but it already triggers the production of the S1 scene
        fpDataFrame['pepsStatus'], fpDataFrame['pepsUrl'] = (
            zip(*[s1Metadata(x).s1PepsStatus(uname, pword) 
                  for x in fpDataFrame.identifier.tolist()]))
        
        # as long as there are any scenes left for downloading, loop
        while len(fpDataFrame[fpDataFrame['pepsStatus'] != 'downloaded']) > 0:
 
            # excluded downlaoded scenes
            fpDataFrame = fpDataFrame[fpDataFrame['pepsStatus'] != 'downloaded']
            
            # recheck for status
            fpDataFrame['pepsStatus'], fpDataFrame['pepsUrl'] = (
            zip(*[s1Metadata(x).s1PepsStatus(uname, pword) 
                  for x in fpDataFrame.identifier.tolist()]))
    
            # if all scenes to download are on Tape, we wait for a minute 
            if len(fpDataFrame[fpDataFrame['pepsStatus'] == 'online']) == 0:
                print('INFO: Imagery still on tape, we will wait for 1 minute and try again.')
                time.sleep(60)
            
            # else we start downloading
            else:
                
                # create the pepslist for parallel download
                pepsList = []
                for index, row in fpDataFrame[fpDataFrame['pepsStatus'] == 'online'].iterrows():   

                    # get scene identifier
                    sceneID = row.identifier
                    # construct download path
                    scene = s1Metadata(sceneID)
                    dwnFile = scene.s1DwnPath(dwnDir)
                    # put all info to the pepslist for parallelised download
                    pepsList.append([fpDataFrame['pepsUrl'].tolist()[0], dwnFile, uname, pword])

                # parallelised download
                pool = multiprocessing.Pool(processes=concurrent)
                pool.map(s1PepsDownload, pepsList)

                # routine to check if the file has been downloaded
                for index, row in fpDataFrame[fpDataFrame['pepsStatus'] == 'online'].iterrows():   

                    # get scene identifier
                    sceneID = row.identifier
                    # construct download path
                    scene = s1Metadata(sceneID)
                    dwnFile = scene.s1DwnPath(dwnDir)
                    if os.path.exists(dwnFile):
                        fpDataFrame.at[index, 'pepsStatus'] = 'downloaded'

                      
# we need this class for earthdata access
class SessionWithHeaderRedirection(requests.Session):

    AUTH_HOST = 'urs.earthdata.nasa.gov'

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    # Overrides from the library to keep headers when redirected to or from
    # the NASA auth host.

    def rebuild_auth(self, prepared_request, response):

        headers = prepared_request.headers
        url = prepared_request.url

        if 'Authorization' in headers:

            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)

            if (original_parsed.hostname != redirect_parsed.hostname) and \
                redirect_parsed.hostname != self.AUTH_HOST and \
                original_parsed.hostname != self.AUTH_HOST:

                del headers['Authorization']

        return


def checkASFConn(uname, pword):

    url = 'https://datapool.asf.alaska.edu/SLC/SA/S1A_IW_SLC__1SSV_20160801T234454_20160801T234520_012413_0135F9_B926.zip'
    session = SessionWithHeaderRedirection(uname, pword)
    response = session.get(url, stream=True)
    return response.status_code


def s1ASFDownload(argumentList):
    """
    This function will download S1 products from ASF mirror.

    :param url: the url to the file you want to download
    :param fileName: the absolute path to where the downloaded file should be written to
    :param uname: ESA's scihub username
    :param pword: ESA's scihub password
    :return:
    """

    url = argumentList[0]
    fileName = argumentList[1]
    uname = argumentList[2]
    pword = argumentList[3]

    session = SessionWithHeaderRedirection(uname, pword)
    #downloaded = False

    #while downloaded = False
    #try:

    print(' INFO: Downloading scene to: {}'.format(fileName))
    # submit the request using the session
    response = session.get(url, stream=True)

    # raise an exception in case of http errors
    response.raise_for_status()

    # get download size
    totalLength = int(response.headers.get('content-length', 0))

    # define chunksize
    chunkSize = 1024

    # check if file is partially downloaded
    if os.path.exists(fileName):
        firstByte = os.path.getsize(fileName)
    else:
        firstByte = 0

    while firstByte < totalLength:

        # get byte offset for already downloaded file
        header = {"Range": "bytes={}-{}".format(firstByte, totalLength)}
        response = session.get(url, headers=header, stream=True)

        # actual download
        with open(fileName, "ab") as f:

            if totalLength is None:
                f.write(response.content)
            else:
                pbar = tqdm.tqdm(total=totalLength, initial=firstByte, unit='B',
                                 unit_scale=True, desc=' INFO: Downloading ')
                for chunk in response.iter_content(chunkSize):
                    if chunk:
                        f.write(chunk)
                        pbar.update(chunkSize)
        pbar.close()

        # updated fileSize
        firstByte = os.path.getsize(fileName)

        #if firstByte >= totalLength:
        #    downloaded = True

        #except requests.exceptions.HTTPError as e:
        #    downloaded = False
            # handle any errors here
        #    print(e)


def downloadS1(inputGDF, dwnDir, concurrent=4):

    print(' INFO: One or more of your scenes need to be downloaded.')
    print(' Select the server from where you want to download:')
    print(' (1) Copernicus Apihub (ESA, rolling archive)')
    print(' (2) Alaska Satellite Facility (NASA, full archive)')
    print(' (3) PEPS (CNES, 1 year rolling archive)')
    #mirror = input(' Type 1, 2 or 3: ')
    mirror = input(' Type 1, 2 or 3: ')

    print(' Please provide username and password for the selected server')
    uname = input(' Username:')
    pword = getpass.getpass(' Password:')

    # check if uname and pwrod are correct
    if mirror == '1':
        errCode = checkApihubConn(uname, pword)
    elif mirror == '2':
        errCode = checkASFConn(uname, pword)
    elif mirror == '3':
        errCode = checkPepsConn(uname, pword)

    if mirror is not '3':
        # check response
        if errCode == 401:
            raise ValueError(' ERROR: Username/Password are incorrect.')
            exit(401)
        elif errCode != 200:
            raise ValueError(' Some connection error.')
            exit(401)
                
        # check if all scenes exist
        scenes = inputGDF['identifier'].tolist()
    
        dowList = []
        asfList = []
    
        for sceneID in scenes:
            scene = s1Metadata(sceneID)
            dlPath = '{}/SAR/{}/{}/{}/{}'.format(dwnDir, scene.product_type,
                                                         scene.year, scene.month, scene.day)
    
            fileName = '{}.zip'.format(scene.scene_id)
    
            uuid = inputGDF['uuid'][inputGDF['identifier'] == sceneID].tolist()
                
            if os.path.isdir(dlPath) is False:
                os.makedirs(dlPath)
    
            # in case the data has been downloaded before
            #if os.path.exists('{}/{}'.format(dlPath, fileName)) is False:
            # create list objects for download
            dowList.append([uuid[0], '{}/{}'.format(dlPath, fileName), uname, pword])
            asfList.append([scene.s1ASFURL(), '{}/{}'.format(dlPath, fileName), uname, pword])
    
                # download in parallel
        if mirror == '1': # scihub
            pool = multiprocessing.Pool(processes=2)
            pool.map(s1ApihubDownload, dowList)
        elif mirror == '2': # ASF
            pool = multiprocessing.Pool(processes=concurrent)
            pool.map(s1ASFDownload, asfList)
    elif mirror is '3': # PEPS
        batchDownloadPeps(inputGDF, dwnDir, uname, pword, concurrent)