import os
import argparse

from ost.s1.grd_to_ard import grd_to_ard

if __name__ == "__main__":
    # write a description
    descript = """
               This is a command line client for the creation of
               Sentinel-1 ARD data from Level 1 GRD products

               Output is a terrain corrected product that is
               calibrated to
                    - Gamma nought (corrected for slopes)
                    - Gamma nought (corrected for ellipsoid)
                    - Sigma nought (corrected for flat terrain)
               """

    epilog = """
             Example:
             grd_to_ard.py -i /path/to/scene -r 20 -p RTC -l True -s False
                        -t /path/to/tmp -o /path/to/search.shp
             """
    # create a parser
    parser = argparse.ArgumentParser(description=descript, epilog=epilog)

    # search paramenters
    parser.add_argument("-i", "--input",
                        help='path to one or more consecutive slices'
                             '(given comma separated list)',
                        required=True, default=None)
    parser.add_argument("-r", "--resolution",
                        help="The output resolution in meters",
                        default=20)
    parser.add_argument("-p", "--producttype",
                        help="The Product Type (RTC, GTCgamma, GTCsigma) ",
                        default='GTCgamma')
    parser.add_argument("-l", "--layover",
                        help="generation of layover/shadow mask (True/False)",
                        default=True)
    parser.add_argument("-s", "--speckle",
                        help="speckle filtering (True/False) ",
                        default=False)
    parser.add_argument("-t", "--tempdir",
                        help="temporary directory (/path/to/temp) ",
                        default='/tmp')
    # output parameters
    parser.add_argument("-o", "--output",
                        help='Output file in BEAM-dimap format. This should'
                             'only be the prefix, since the workflow will'
                             'add the file suffixes on its own.',
                        required=True)

    args = parser.parse_args()

    # create args for grd_to_ard
    infiles = args.input.split(',')
    output_dir = os.path.dirname(args.output)
    file_id = os.path.basename(args.output)

    # execute processing
    grd_to_ard(infiles, output_dir, file_id, args.tempdir,
               int(args.resolution), args.producttype, args.ls_mask,
               args.speckle_filter)