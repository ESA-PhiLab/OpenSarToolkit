import logging
import environs
import datetime
from ost.errors import OSTConfigError

logger = logging.getLogger(__name__)

# read environment variables
ENV = environs.Env()
ENV.read_env()

# mapping of ENV types
ENVTYPES = {
    str: ENV.str,
    bool: ENV.bool,
    int: ENV.int,
    float: ENV.float,
    list: ENV.list,
    dict: ENV.dict,
    datetime.date: ENV.datetime
}


class ConfigParam():
    def __init__(
            self,
            type=None,
            default=None,
            conditional_default=None,
            choice=None,
            required=False,
            parser=None
    ):
        self.default = default
        self.conditional_default = conditional_default
        self.required = required
        self.parser = parser
        self.type = type
        self.choice = choice

    def __repr__(self):
        return (
           "ConfigParam(type=%s, default=%s, choice=%s, sub_params=%s, required=%s)"
               ) % (
           self.type, self.default, self.choice, self.sub_params, self.required
               )

    def parse(self, value):
        # throw errors if value is not as expected
        if value is None and self.required:
            raise OSTConfigError("parameter required")

        value = self.parser(value) if self.parser else value

        if isinstance(value, int) and self.type == float:
            value = float(value)

        if not isinstance(value, self.type):
            raise TypeError(
                "value must be type %s, not '%s' (%s)" % (self.type, value, type(value))
            )
        if self.choice and value not in self.choice:
            raise ValueError("value must be one of %s, not %s" % (self.choice, value))
        return value


class ConfigParamGroup():
    def __init__(
            self, sub_params=None, required=False
    ):
        self.sub_params = sub_params
        self.required = required

    def __repr__(self):
        return "ConfigParamGroup(required=%s, sub_params=%s)" % (
            self.required, self.sub_params
        )

    def __len__(self):
        return len(self.sub_params)

    def __iter__(self):
        return iter(self.sub_params)


def parse_config(input_params=None, default_config=None, prefix=""):
    out = {}
    for param, value in default_config.items():
        if isinstance(value, ConfigParamGroup):
            if value.required or param in input_params:
                out[param] = parse_config(
                    input_params=input_params.get(param, {}),
                    default_config=value.sub_params,
                    prefix=param.upper() + "_"
                )
            else:
                out[param] = {}
        else:
            out[param] = get_param(
                param_name=param,
                process_config=input_params,
                default_config=default_config,
                prefix=prefix
            )
    return out


def get_param(param_name=None, process_config=None, default_config=None, prefix=""):
    """
    Return correct configuration value

    Use environmental variables starting with 'MP_SATELLITE_', then values from
    process configuration, then default values.
    """
    if param_name not in default_config:
        raise OSTConfigError("%s is not a valid parameter" % param_name)
    env_prefix = "OST_" + prefix
    # get target type
    target_type = default_config[param_name].type

    # (1) get value from environment
    with ENV.prefixed(env_prefix):
        try:
            value = ENVTYPES[target_type](param_name.upper())
            src = "environment"
        except environs.EnvError:
            value = None
    # (2) or from process config
    if value is None:
        value = process_config.get(param_name, None)
        src = "process config"
    # (3) or from default
    if value is None:
        condition = default_config.get(param_name).conditional_default
        if condition:
            conditional_param, conditional_defaults = condition
            conditional_key = get_param(
                # key of conditional default value
                param_name=conditional_param,
                process_config=process_config,
                default_config=default_config
            )
            value = conditional_defaults[conditional_key]
        else:
            value = default_config.get(param_name).default
        src = "default"

    # validate and return
    try:
        value = default_config[param_name].parse(value)
    except Exception as e:
        raise OSTConfigError("error on parameter '%s': %s" % (param_name, e))

    logger.debug("use %s value: %s=%s", src, param_name, value)
    return value


SNAP_S1_RESAMPLING_METHODS = [
    'NEAREST_NEIGHBOUR',
    'BILINEAR_INTERPOLATION',
    'CUBIC_CONVOLUTION',
    'BISINC_5_POINT_INTERPOLATION',
    'BISINC_11_POINT_INTERPOLATION',
    'BISINC_21_POINT_INTERPOLATION',
    'BICUBIC_INTERPOLATION',
    # 'DELAUNAY_INTERPOLATION' not in TF Operator as of 2019-11
    ]

SINGLE_ARD_OPTIONS = ['CEOS', 'Earth Engine', 'OST Standard']
ARD_PRODUCT_TYPES = ['RTC', 'GTCgamma', 'GTCsigma']
POLARIZATION_OPTIONS = ['VV', 'VV VH', 'HH', 'HH HV']

DEM_NAMES = ["SRTM 1Sec HGT", "External DEM"]

SPECKLE_FILTERS = ['None', 'Boxcar', 'Median', 'Frost', 'Gamma Map',
                   'Lee', 'Refined Lee', 'Lee Sigma', 'IDAN']
SIGMA_LEE = [0.5, 0.6, 0.7, 0.8, 0.9]
WINDOW_SIZES = ['3x3', '5x5']
TARGET_WINDOW_SIZES = ['5x5', '7x7', '9x9', '11x11', '13x13', '15x15', '17x17']

SINGLE_ARD_PROCESSING_PARAMETERS = {
    "type": ConfigParam(type=str, default="OST Standard",
                        required=True, choice=SINGLE_ARD_OPTIONS
                        ),
    "multitemporal": ConfigParam(type=bool, default=False, required=True),
    "resolution": ConfigParam(type=float, default=20, required=True),
    "remove_border_noise": ConfigParam(type=bool, default=True),
    "product_type": ConfigParam(type=str, default='GTCgamma', choice=ARD_PRODUCT_TYPES),
    "polarization":  ConfigParam(type=str, default='VV VH', choice=POLARIZATION_OPTIONS),
    "to_db": ConfigParam(type=bool, default=False),
    "remove_speckle": ConfigParam(type=bool, default=False),
    "speckle_filter": ConfigParamGroup(sub_params={
        "filter": ConfigParam(type=str, default='Refined Lee', choice=SPECKLE_FILTERS),
        "ENL": ConfigParam(type=int, default=1),
        "estimate_ENL": ConfigParam(type=bool, default=True),
        "sigma": ConfigParam(type=float, default=0.9, choice=SIGMA_LEE),
        "filter_x_size": ConfigParam(type=int, default=3, choice=list(range(1, 101))),
        "filter_y_size": ConfigParam(type=int, default=3, choice=list(range(1, 101))),
        "window_size": ConfigParam(type=str, default="3x3", choice=WINDOW_SIZES),
        "target_window_size": ConfigParam(
            type=str, default="7x7", choice=TARGET_WINDOW_SIZES
        ),
        "num_of_looks": ConfigParam(type=int, default=1, choice=list(range(1, 5))),
        "damping": ConfigParam(type=int, default=2, choice=list(range(1, 101))),
        "pan_size": ConfigParam(type=int, default=50, choice=list(range(1, 201)))
    }
    ),
    "create_ls_mask": ConfigParam(type=bool, default=True),
    "apply_ls_mask": ConfigParam(type=bool, default=False),
    "dem": ConfigParamGroup(sub_params={
        "dem_name": ConfigParam(type=str, default="SRTM 1Sec HGT", choice=DEM_NAMES),
        "dem_file": ConfigParam(type=str, default=""),
        "dem_nodata": ConfigParam(type=float, default=0.0),
        "dem_resampling": ConfigParam(
            type=str, default='BILINEAR_INTERPOLATION', choice=SNAP_S1_RESAMPLING_METHODS
        ),
        "image_resampling": ConfigParam(
            type=str, default='BILINEAR_INTERPOLATION', choice=SNAP_S1_RESAMPLING_METHODS
        )
    }
    )
}
