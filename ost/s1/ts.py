
# import stdlib modules
import os
import sys
import glob
import pkg_resources
from datetime import datetime



from ost.helpers import helpers

# get the SNAP CL executable
global gpt_file
gpt_file = helpers.getGPT()
# define the resource package for getting the xml workflow files
global package
package = 'ost'


def createStackPol(fileList, polarisation, outStack, logFile, wkt=None):
    '''

    :param fileList: list of single Files (space separated)
    :param outFile: the stack that is generated
    :return:
    '''

    if wkt is None:
        graph = ('/'.join(('graphs', 'S1_TS', '1_BS_Stacking.xml')))
        graph = pkg_resources.resource_filename(package, graph)

        print(" INFO: Creating multi-temporal stack of images")
        stackCmd = '{} {} -x -q {} -Pfilelist={} -Ppol={} \
               -Poutput={}'.format(gpt_file, graph, os.cpu_count(),
                                   fileList, polarisation, outStack)
    else:
        # does not work with gpt at the moment
        graph = ('/'.join(('graphs', 'S1_TS', '1_BS_Stacking_Subset.xml')))
        graph = pkg_resources.resource_filename(package, graph)

        print(" INFO: Creating multi-temporal stack of images")
        stackCmd = '{} {} -x -q {} -Pfilelist={} -Ppol={} \
               -Pwkt=\'{}\' -Poutput={}'.format(gpt_file, graph,
                                                os.cpu_count(), fileList,
                                                polarisation, wkt, outStack)
                                            
    #print(stackCmd)
    rc = helpers.runCmd(stackCmd, logFile)

    if rc == 0:
        print(' INFO: Succesfully created multi-temporal stack')
    else:
        print(' ERROR: Stack creation exited with an error.'
              ' See {} for Snap Error output'.format(logFile))
        sys.exit(201)


def mtSpeckle(inStack, outStack, logFile):
    """

    :param inStack:
    :param outStack:
    :return:
    """

    graph = ('/'.join(('graphs', 'S1_TS', '2_MT_Speckle.xml')))
    graph = pkg_resources.resource_filename(package, graph)

    print(" INFO: Applying the multi-temporal speckle-filtering")
    mtSpkFltCmd = '{} {} -x -q {} -Pinput={} \
                   -Poutput={}'.format(gpt_file, graph, os.cpu_count(),
                                       inStack, outStack)

    rc = helpers.runCmd(mtSpkFltCmd, logFile)

    if rc == 0:
        print(' INFO: Succesfully applied multi-temporal speckle filtering')
    else:
        print(' ERROR: Multi-temporal speckle filtering exited with an error. \
                See {} for Snap Error output'.format(logFile))
        sys.exit(202)


def createDateList(tsPath):

    files = glob.glob('{}*VV*tif'.format(tsPath))
    dates = sorted([os.path.basename(file).split('.')[1] for file in files])
    #outDates = [datetime.strftime(datetime.strptime(date,  '%y%m%d'), '%Y-%m-%d') ]
    f = open('{}/datelist.txt'.format(tsPath), 'w')
    for date in dates:
        f.write(str(datetime.strftime(datetime.strptime(date,  '%y%m%d'), '%Y-%m-%d')) + ' \n')        
    f.close()
    