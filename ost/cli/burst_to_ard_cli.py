import os
import argparse

from ost.s1.burst_to_ard import burst_to_ard

if __name__ == "__main__":
    # write a description
    descript = """
              This is a command line client for the creation of
              Sentinel-1 ARD data from Level 1 SLC bursts
    
              to do
              """
    epilog = """
            Example:
            to do
    
    
            """
    # create a parser
    parser = argparse.ArgumentParser(description=descript, epilog=epilog)

    # search paramenters
    parser.add_argument('-m', '--master',
                        help='(str) path to the master SLC',
                        required=True
                        )
    parser.add_argument('-mn', '--master_burst_nr',
                        help='(int) The index number of the master burst',
                        required=True
                        )
    parser.add_argument('-mi', '--master_burst_id',
                        help='(str) The OST burst id of the master burst'
                        )
    parser.add_argument('-s', '--slave',
                        help='(str) path to the slave SLC'
                        )
    parser.add_argument('-sn', '--slave_burst_nr',
                        help='(int) The index number of the slave burst'
                        )
    parser.add_argument('-si', '--slave_burst_id',
                        help='(str) The OST burst id of the slave burst'
                        )
    parser.add_argument('-o', '--out-directory',
                        help='The directory where the outputfiles will'
                             'be written to.',
                        required=True
                        )
    parser.add_argument('-t', '--tempdir',
                        help='The directory where temporary files will'
                             'be written to.',
                        required=True
                        )
    parser.add_argument('-coh', '--coherence',
                        help='(bool) Set to True if the interferometric '
                             'coherence should be calculated.',
                        default=True
                        )
    parser.add_argument('-pol', '--polarimetric-decomposition',
                        help='(bool) (bool) Set to True if the polarimetric '
                             'H/A/Alpha decomposition should be calculated.',
                        default=True
                        )
    parser.add_argument('-ps', '--polarimetric-speckle-filter',
                        help='(bool) Set to True if speckle filtering should'
                             'be applied on the polarimetric'
                             'H/A/Alpha decomposition',
                        default=True
                        )
    parser.add_argument('-ls', '--ls-mask',
                        help='(bool) Set to True for the creation of the'
                             'layover/shadow mask.',
                        default=True
                        )
    parser.add_argument('-sp', "--speckle-filter",
                        help='(bool) Set to True if speckle filtering on'
                             'backscatter should be applied.',
                        default=False
                        )
    parser.add_argument('-r', '--resolution',
                        help='(int) Resolution of the desired output data'
                             'in meters',
                        default=20
                        )
    parser.add_argument('-pt', '--product-type',
                        help='(str) The product type of the desired output'
                             'in terms of calibrated backscatter'
                             '(i.e.  either GTCsigma, GTCgamma, RTC)',
                        default='RTC')
    parser.add_argument('-db', '--to-decibel',
                        help='(bool) Set to True if the desied output should'
                             'be in dB scale',
                        default=False
                        )
    parser.add_argument('-d', '--dem',
                        help='(str) Select the DEM for processing steps where'
                             'the terrain information is needed.'
                             '(Snap format)',
                        default='SRTM 1Sec HGT'
                        )
    parser.add_argument('-rsi', '--remove-slave-import',
                        help='(bool) Select if during the coherence'
                             'calculation the imported slave file should be'
                             'deleted (for time-series it is advisable to'
                             'keep it)',
                        default=False
                        )

    args = parser.parse_args()

    # create args for grd_to_ard
    infiles = args.input.split(',')
    output_dir = os.path.dirname(args.out-directory)
    file_id = os.path.basename(args.output)

    # execute processing
    burst_to_ard(infiles,
                 output_dir,
                 file_id,
                 args.tempdir,
                 int(args.resolution),
                 args.producttype,
                 args.ls_mask,
                 args.speckle_filter
                 )
