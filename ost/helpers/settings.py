import os
import sys
import getpass
import shutil
import importlib.util
import logging
from pathlib import Path
import numpy as np


# ---------------------------------------------------------------
# 1 logger set-up

# lower stream output log level
class SingleLevelFilter(logging.Filter):
    def __init__(self, passlevel, reject):
        self.passlevel = passlevel
        self.reject = reject

    def filter(self, record):
        if self.reject:
            return record.levelno != self.passlevel
        else:
            return record.levelno == self.passlevel


formatter = logging.Formatter(" %(levelname)s (%(asctime)s): %(message)s", "%H:%M:%S")
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.DEBUG)


def set_log_level(log_level=logging.INFO):

    # if set to logging.INFO then only show logging.info
    if log_level == logging.INFO:
        info_filter = SingleLevelFilter(logging.INFO, False)
        stream_handler.addFilter(info_filter)
        logging.getLogger().addHandler(stream_handler)

    logging.getLogger("ost").setLevel(log_level)
    stream_handler.setLevel(log_level)


def setup_logfile(logfile):
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)


def exception_handler(exception_type, exception, traceback):
    print(f"{exception_type.__name__}, {exception}", file=sys.stderr)


# ---------------------------------------------------------------


# ---------------------------------------------------------------
# The check on the ARD config files
def check_value(key, value, expected_type, choices=None):
    if not isinstance(value, expected_type):
        raise TypeError(
            "ARD parameter {} does not have the right type {}. "
            "It should be {}.".format(key, value, str(expected_type))
        )

    if key == "metrics":
        all(item in value for item in choices)

    elif choices:
        if value not in choices:
            raise ValueError(
                "Configuration value for ARD parameter {} is wrong {}. "
                "It should be one of: {}".format(key, value, choices)
            )

    return "passed"


# fully loop through dict until value
def check_ard_parameters(ard_parameters):
    # loop through dict
    for key, value in ard_parameters.items():

        # if our value is a dict, re-apply the function
        # until we get to the actual value level
        if isinstance(value, dict):
            check_ard_parameters(value)
        else:
            if key == "dem_file":
                if value != " ":
                    check_value(key, value, config_check[key]["type"])
                    if not Path(value).exists():
                        raise FileNotFoundError("External DEM File not found.")
            # otherwise we do the check routine
            else:
                if config_check[key]["type"] is bool:
                    check_value(key, value, config_check[key]["type"])
                else:
                    check_value(
                        key,
                        value,
                        config_check[key]["type"],
                        config_check[key]["choices"],
                    )


# ---------------------------------------------------------------
# locate SNAP'S gpt file
def get_gpt():
    gpt_file = None

    # on Windows
    if os.name == "nt":
        if Path(r"c:/Program Files/snap/bin/gpt.exe").is_file() is True:
            gpt_file = Path(r"c:/Program Files/snap/bin/gpt.exe")
        else:
            gpt_file = input(
                r" Please provide the full path to the"
                r" SNAP gpt command line executable"
                r" (e.g. C:\path\to\snap\bin\gpt.exe)"
            )
            gpt_file = Path(gpt_file)

            if not gpt_file.exists():
                raise FileNotFoundError("Given path to gpt does not exist.")

    # Unix systems (Mac, Linux)
    else:
        # possible UNIX paths
        paths = [
            Path.home() / ".ost" / "gpt",
            Path.home() / "snap" / "bin" / "gpt",
            Path.home() / "programs" / "snap" / "bin" / "gpt",
            Path("/home/ost/programs/snap/bin/gpt"),
            Path("/usr/bin/gpt"),
            Path("/opt/snap/bin/gpt"),
            Path("/usr/local/snap/bin/gpt"),
            Path("/usr/local/lib/snap/bin/gpt"),
            Path("/usr/programs/snap/bin/gpt"),
            Path("/Applications/snap/bin/gpt"),
        ]

        # loop trough possible paths and see if we find it
        for path in paths:
            if path.exists():
                gpt_file = path
                break
            else:
                gpt_file = None

    # check if we have an environmental variable that contains the path to gpt
    if not gpt_file:
        gpt_file = os.getenv("GPT_PATH")

    # we search with bash's which
    if not gpt_file:
        try:
            gpt_file = Path(shutil.which("gpt"))
            gpt_file.exists()
        except Exception:
            # we give up and ask the user
            gpt_file = input(
                " Please provide the full path to the SNAP"
                " gpt command line executable"
                " (e.g. /path/to/snap/bin/gpt) or leave empty"
                " if you just want to use the"
                " OST inventory and download routines."
            )

        if not gpt_file:
            gpt_file = ""
        elif not Path(gpt_file).exists():
            raise FileNotFoundError("Given path to gpt does not exist.")
        else:
            # if file exists we copy to one of the possible paths, so next time
            # we will find it right away
            (Path.home() / ".ost").mkdir(exist_ok=True)
            if not (Path.home() / ".ost" / "gpt").exists():
                os.symlink(gpt_file, Path.home() / ".ost" / "gpt")
            gpt_file = Path.home() / ".ost" / "gpt"

    return str(gpt_file)


# set global variable for gpt path
GPT_FILE = get_gpt()

# set global variable for root directory of OST
OST_ROOT = Path(importlib.util.find_spec("ost").submodule_search_locations[0])

# set global variable for APIHUB product
APIHUB_BASEURL = "https://apihub.copernicus.eu/apihub/odata/v1/Products"
# ---------------------------------------------------------------

# ---------------------------------------------------------------
# dummy user for tests
HERBERT_USER = {
    "uname": "herbert_thethird",
    "pword": "q12w34er56ty7",
    "asf_pword": "q12w34er56ty7WER32P",
}

config_check = dict(
    {
        "image_type": {"type": str, "choices": ["GRD", "SLC"]},
        "ard_type": {
            "type": str,
            "choices": ["OST-GTC", "OST-RTC", "Earth-Engine", "CEOS"],
        },
        "resolution": {"type": int, "choices": range(10, 5000)},
        "backscatter": {"type": bool},
        "remove_border_noise": {"type": bool},
        "product_type": {
            "type": str,
            "choices": ["GTC-sigma0", "GTC-gamma0", "RTC-gamma0"],
        },
        "polarisation": {
            "type": str,
            "choices": ["VV, VH, HH, HV", "VV", "VH", "VV, VH", "HH, HV", "VV, HH"],
        },
        "to_db": {"type": bool},
        "to_tif": {"type": bool},
        "geocoding": {"type": str, "choices": ["terrain", "ellipsoid"]},
        "remove_speckle": {"type": bool},
        "filter": {
            "type": str,
            "choices": [
                "None",
                "Boxcar",
                "Median",
                "Frost",
                "Gamma Map",
                "Lee",
                "Refined Lee",
                "Lee Sigma",
                "IDAN",
            ],
        },
        "ENL": {"type": int, "choices": range(1, 500)},
        "estimate_ENL": {"type": bool},
        "sigma": {
            "type": float,
            "choices": [np.round(i, 1) for i in np.arange(0.5, 1, 0.1)],
        },
        "filter_x_size": {"type": int, "choices": range(1, 100)},
        "filter_y_size": {"type": int, "choices": range(1, 100)},
        "window_size": {
            "type": str,
            "choices": ["5x5", "7x7", "9x9", "11x11", "13x13", "15x15", "17x17"],
        },
        "target_window_size": {"type": str, "choices": ["3x3", "5x5"]},
        "num_of_looks": {"type": int, "choices": range(1, 4)},
        "damping": {"type": int, "choices": range(0, 100)},
        "pan_size": {"type": int, "choices": range(1, 200)},
        "remove_pol_speckle": {"type": bool},
        "polarimetric_filter": {
            "type": str,
            "choices": [
                "Box Car Filter",
                "IDAN Filter",
                "Refined Lee Filter",
                "Improved Lee Sigma Filter",
            ],
        },
        "filter_size": {"type": int, "choices": range(1, 100)},
        "search_window_size": {"type": int, "choices": [i for i in range(3, 27, 2)]},
        "scale_size": {"type": int, "choices": range(0, 2)},
        "create_ls_mask": {"type": bool},
        "dem_name": {
            "type": str,
            "choices": [
                "Copernicus 30m Global DEM",
                "Copernicus 90m Global DEM",
                "SRTM 1Sec HGT",
                "SRTM 3Sec",
                "Aster 1sec GDEM",
                "GETASSE30",
                "External DEM",
            ],
        },
        "dem_file": {"type": str},
        "dem_nodata": {"type": int, "choices": range(0, 66000)},
        "dem_resampling": {
            "type": str,
            "choices": [
                "NEAREST_NEIGHBOUR",
                "BILINEAR_INTERPOLATION",
                "CUBIC_CONVOLUTION",
                "BISINC_5_POINT_INTERPOLATION",
                "BISINC_11_POINT_INTERPOLATION",
                "BISINC_21_POINT_INTERPOLATION",
                "BICUBIC_INTERPOLATION",
                "DELAUNAY_INTERPOLATION",
            ],
        },
        "image_resampling": {
            "type": str,
            "choices": [
                "NEAREST_NEIGHBOUR",
                "BILINEAR_INTERPOLATION",
                "CUBIC_CONVOLUTION",
                "BISINC_5_POINT_INTERPOLATION",
                "BISINC_11_POINT_INTERPOLATION",
                "BISINC_21_POINT_INTERPOLATION",
                "BICUBIC_INTERPOLATION",
            ],
        },
        "egm_correction": {"type": bool},
        "out_projection": {"type": int, "choices": range(2000, 42002)},
        "coherence": {"type": bool},
        "coherence_bands": {
            "type": str,
            "choices": ["VV, VH, HH, HV", "VV", "VH", "VV, VH", "HH, HV", "VV, HH"],
        },
        "coherence_azimuth": {"type": int, "choices": range(1, 100)},
        "coherence_range": {"type": int, "choices": range(1, 500)},
        "production": {"type": bool},
        "H-A-Alpha": {"type": bool},
        "apply_ls_mask": {"type": bool},
        "remove_mt_speckle": {"type": bool},
        "deseasonalize": {"type": bool},
        "dtype_output": {"type": str, "choices": ["float32", "uint8", "uint16"]},
        "metrics": {
            "type": list,
            "choices": [
                "median",
                "percentiles",
                "harmonics",
                "avg",
                "max",
                "min",
                "std",
                "cov",
            ],
        },
        "remove_outliers": {"type": bool},
        "harmonization": {"type": bool},
        "cut_to_aoi": {"type": bool},
    }
)


def generate_access_file():
    access_dict = {
        "scihub": {"un": None, "pw": None},
        "asf": {"un": None, "pw": None},
        "peps": {"un": None, "pw": None},
        "onda": {"un": None, "pw": None},
    }

    # Asking for scihub
    access_dict["scihub"]["username"] = input("")
    getpass.getpass("")

    # Asking for ASF

    # Asking for CNES' Peps

    # Asking for ONDA
