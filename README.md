# Open SAR Toolkit (OST)

## Objective

This python package lowers the entry barrier for accessing and pre-processing 
Sentinel-1 data for land applications and allows users with little knowledge 
on SAR and python to produce various Analysis-Ready-Data products.

## Functionality

The Open SAR Toolkit (OST) bundles the full workflow for the generation of 
Analysis-Ready-Data (ARD) of Sentinel-1 for Land in a single high-level 
python package. It includes functions for data inventory and advanced sorting 
as well as downloading from various mirrors. The whole pre-processing is 
bundled in a single function and different types of ARD can be selected,
but also customised. OST does include advanced types of ARD such as combined
production of calibrated backscatter, interferometric coherence and the dual-
polarimetric H-A-Alpha decomposition. Time-series and multi-temporal statistics
(i.e. timescans) can be produced for each of these layers and the generation of 
sptaially-seamless large-scale mosaic over time is possible a well.

The Open SAR Toolkit realises this by using an object-oriented approach, 
providing classes for single scene processing, GRD and SLC batch processing 
routines. The SAR processing itself relies on ESA's Sentinel-1 Toolbox as well 
as some geospatial python libraries and the Orfeo Toolbox for mosaicking.

You can find examplarotary Jupyter notebooks at 
https://github.com/ESA-PhiLab/OST_Notebooks for getting started. 

## Installation

### Docker 

A docker image is available from docker hub that contains the full package, 
including ESA's Sentinel-1 Toolbox, Orfeo Toolbox, Jupyter Lab as well
as the Open SAR Toolkit the tutorial notebooks.

Docker installation is possible on various OS. Installation instructions can be 
found at https://docs.docker.com/install/

After docker is installed and running, launch the container with 
(adapt the path to the shared host folder):

```
docker pull buddyvolly/opensartoolkit
docker run -it -p 8888:8888 -v /shared/folder/on/host:/home/ost/shared buddyvolly/opensartoolkit
```

The docker image automatically executes the jupyter lab and runs it on 
port 8888. You can find the address to the notebook on the command line where 
docker is running. Copy it into your favorites browser and replace 
127.0.0.1 with localhost.


### Manual installation

#### Dependencies

##### Sentinel Application Toolbox (SNAP)

OST bases mainly on the freely available SNAP toolbox for the 
SAR-specific processing routines. You can download SNAP from:

http://step.esa.int/main/download/

If you install SNAP into the standard directory, OST should have no problems 
to find the SNAP command line executable. Otherwise you need to define the path 
to the gpt file on your own during processing.

##### Orfeo Toolbox

If you want to create mosaics between different swaths, OST will rely on the 
otbcli_Mosaic command from The Orfeo Toolbox. You can download Orfeo from:

https://www.orfeo-toolbox.org/download/

Make sure that the Orfeo bin folder is within your PATH variable to allow 
execution from command line.

#### OST installation

OST is developed under Ubuntu 18.04 OS in python 3.6. It has not been tested 
much on other OS and python versions, but should in principle work on any OS 
and any python version >= 3.5.

##### Ubuntu/Debian Linux (using pip)

Before installation of OST, run the following line on the terminal to 
install further dependencies:

```
sudo apt install python3-pip git libgdal-dev python3-gdal libspatialindex-dev
```

then install OST as a global package (for all users, admin rights needed):

```
sudo pip3 install git+https://github.com/ESA-PhiLab/OpenSarToolkit.git
```

or as local package within your home folder (no admin rights needed):

```
pip3 install --user git+https://github.com/ESA-PhiLab/OpenSarToolkit.git
```


##### Mac OS (using homebrew/pip)

If not already installed, install homebrew as explained on https://brew.sh

After installation of homebrew, open the terminal and install 
further dependecies:

```
brew install python3 gdal2 gdal2-python git
```

then install OST with python pip:
```
pip3 install git+https://github.com/ESA-PhiLab/OpenSarToolkit.git
```


##### Conda Installation (Windows, Mac, Linux)

Follow the installation instructions for conda (Miniconda is sufficient) at:
https://docs.conda.io/projects/conda/en/latest/user-guide/install/

Then run the conda command to install OST's dependencies:
```
conda install pip gdal jupyter jupyterlab git matplotlib numpy rasterio imageio rtree geopandas fiona shapely matplotlib descartes tqdm scipy
```

Finally get the OST by using pip 
(we will work in future on a dedicated conda package for OST).
```
pip install git+https://github.com/ESA-PhiLab/OpenSarToolkit.git
```


## Examples

### Ecuador VV-polarised Timescan Composite

Year: 2016

Sensor: Sentinel-1 C-Band SAR.

Acquisitions: 6 acquisitions per swath (4 swaths)

Output resolution: 30m

RGB composite:
  - Red: VV-maximum
  - Green: VV-minimum
  - Blue: VV-Standard deviation

![alt text](https://github.com/openforis/opensarkit/raw/master/shiny/www/ecuador_VV_max_min_std.png)

### Ethiopia VV-VH polarised Timescan Composite

Year: 2016-2017

Sensor: Sentinel-1 C-Band SAR.

Acquisitions: 7 acquisitions per swath (about 400 scenes over 8 swaths)

Output resolution: 30m

RGB composite:
  - Red: VV-minimum
  - Green: VH-minimum
  - Blue: VV-Standard deviation

![alt text](https://github.com/openforis/opensarkit/raw/master/shiny/www/eth_vvvh_ts.jpeg)


## A note on its origin

Open SAR Toolkit was initially developed at the Food and Agriculture 
Organization of the United Nations under the SEPAL project 
(https://github.com/openforis/sepal) between 2016-2018. 
It is still available there (https://github.com/openforis/opensarkit), 
but has been completely re-factored and transferred into a simpler and 
less-dependency rich python3 version, which can be found on this page here. 
Instead of using R-Shiny as a GUI, the main interface are now Jupyter notebooks 
that are developed in parallel to this core package and should help to get started.
(https://github.com/ESA-PhiLab/OST_Notebooks) 


## Author

- Andreas Vollrath, ESA
