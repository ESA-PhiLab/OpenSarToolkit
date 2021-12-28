from pathlib import Path
from setuptools import setup, find_packages
from setuptools.command.develop import develop
from subprocess import check_call

# check that gdal has already been installed prior to ost
try:
    from osgeo import gdal
except ModuleNotFoundError:
    raise ImportError(
        "please install a GDAL distribution>=1.7 prior to install OpenSarTollkit, we recommand using the pygdal packages"
    )

# to make flake8 happy
gdal.__version__

# the version number
version = "0.12.15"

# The directory containing this file
HERE = Path(__file__).parent

DESCRIPTION = "High-level functionality for the inventory, download and pre-processing of Sentinel-1 data"
LONG_DESCRIPTION = (HERE / "README.rst").read_text()


class DevelopCmd(develop):
    """overwrite normal develop pip command to install the pre-commit"""

    def run(self):
        check_call(
            [
                "pre-commit",
                "install",
                "--install-hooks",
                "-t",
                "pre-commit",
                "-t",
                "commit-msg",
            ]
        )
        super(DevelopCmd, self).run()


setup(
    name="opensartoolkit",
    version=version,
    license="MIT License",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/x-rst",
    author="Andreas Vollrath",
    author_email="opensarkit@gmail.com",
    url="https://github.com/ESA-PhiLab/OpenSarToolkit",
    download_url=f"https://github.com/ESA-PhiLab/OpenSarToolkit/archive/{version}.tar.gz",
    keywords=[
        "Sentinel-1",
        "ESA",
        "SAR",
        "Radar",
        "Earth Observation",
        "Remote Sensing",
        "Synthetic Aperture Radar",
    ],
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "pyproj",
        "descartes",
        "godale",
        "geopandas>=0.8",
        "jupyterlab",
        "psycopg2-binary",
        "rasterio",
        "requests",
        "scipy",
        "tqdm",
        "imageio",
        "rtree",
        "retrying",
    ],
    extras_require={
        "dev": [
            "pre-commit",
            "pytest",
            "coverage",
            "nbsphinx",
            "pydata-sphinx-theme",
            "sphinx-copybutton",
            "Commitizen",
        ]
    },
    cmdclass={"develop": DevelopCmd},
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering :: GIS",
    ],
)
