# -*- coding: utf-8 -*-
import getpass
import urllib


def askPepsCreds():
    
    # SciHub account details (will be asked by execution)
    print(' If you do not have a CNES Peps user account'
          ' go to: https://peps.cnes.fr/ and register')
    uname = input(' Your CNES Peps Username:')
    pword = getpass.getpass(' Your CNES Peps Password:')
    
    return uname, pword


def pepsConnect(uname=None, pword=None):
    """
    Connect and authenticate to the scihub server.
    """
    
    baseURL = 'https://peps.cnes.fr/'
    
    if uname == None:
        print(' If you do not have a CNES Peps user account'
          ' go to: https://peps.cnes.fr/ and register')
    uname = input(' Your CNES Peps Username:')
    
    if pword == None:
        pword = getpass.getpass(' Your CNES Peps Password:')
        
    # open a connection to the scihub
    manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    manager.add_password(None, baseURL, uname, pword)
    handler = urllib.request.HTTPBasicAuthHandler(manager)
    opener = urllib.request.build_opener(handler)

    return opener