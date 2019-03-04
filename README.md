# Open SAR Toolkit (OST)

## A note on its origin

Open SAR Toolkit was initially developed at the Food and Agriculture Organization of the United Nations under the SEPAL project (https://github.com/openforis/sepal) between 2016-2018. It is still available there (https://github.com/openforis/opensarkit), but has been completely re-factored and transferred into a simpler and less-dependency rich python3 version, which can be found on this page here. Instead of using R-Shiny as a GUI, the main interface are now Jupyter notebooks that are developed in parallel to this core package and should help to get started.

## Objective

Compared to its optical counterpart, the community of Synthetic Aperture Radar (SAR) data users for land applications is still small. One major reason for that originates from the differences in the acquisition principle and the underlying physics of the imaging process. For non-experts, this results in difficulties of applying the correct processing steps as well as interpreting the non-intuitive backscatter image composites. On the other hand, the free and open access to Sentinel-1 data widened the community of interested users and paves the way for the integration of SAR data into operational monitoring systems.

This python package lowers the entry barrier for pre-processing Sentinel-1 data and allows users with little knowledge on SAR and python to produce analysis-ready small to large-scale SAR datasets. OST includes fully automated routines that are mainly build on top of the Sentinel Application Platform (SNAP) and other freely available open-source software such as GDAL, Orfeo Toolbox and Python.

## Functionality

For the moment, Sentinel-1 data inventory and download routines, as well as a GRD to RTC processor allows for the rapid generation of radiometrically terrain corrected (RTC) imagery that is ready for subsequent analysis tasks such as land cover classification. More advanced and processing intensive data products, such as time-series and timescan imagery can be easily produced as well in a fully automatic manner. Ultimately, mosaicking generates seamless wide-area data sets.

## Installation

OST is rather a meta-package of the Sentinel-1 toolbox than a full-flavoured software. 
In order to make 

### Dependencies (OS independent)

#### Sentinel Application Toolbox (SNAP)

OST bases mainly on the freely available SNAP toolbox for the SAR-specific processing routines. You can download SNAP from:

http://step.esa.int/main/download/

If you install SNAP into the standard directory, OST should have no problems to find the SNAP command line executable. Otherwise you need to define the path to the gpt file on your own during processing.

#### Orfeo Toolbox

If you want to create mosaics between different swaths, OST will rely on the otbcli_Mosaic command from The Orfeo Toolbox. You download Orfeo from:

https://www.orfeo-toolbox.org/download/

Make sure that the Orfeo bin folder is within your PATH variable to allow execution from command line.


OST is developed under Ubuntu 18.04 OS in python 3.6. It has not been tested much on other OS and python versions,
but should in principle work on any OS and any python version >= 3.5.

Before it can work, some dependencies need to be installed:


### Ubuntu/Debian Linux (using pip)

Before installation of OST, run the following line on the terminal:

```
sudo apt install python3-pip git libgdal-dev python3-gdal libspatialindex-dev
```

then isntall OST as a global package (for all users, admin rights needed):

```
sudo pip3 install git+https://github.com/ESA-PhiLab/OpenSarToolkit.git
```

or as local package within your home folder (no admin rights needed):

```
pip3 install --user git+https://github.com/ESA-PhiLab/OpenSarToolkit.git
```


### Mac OS (using homebrew/pip)

If not already installed, install homebrew as explained on https://brew.sh

After installation of homebrew, open the terminal and execute this:
```
brew install python3 gdal2 gdal2-python git
```

and this:
```
pip3 install git+https://github.com/ESA-PhiLab/OpenSarToolkit.git
```


### Conda Installation (Windows, Mac, Linux)

Download miniconda3 (python version 3) from https://conda.io/miniconda.html 
and install it.

Run: 
```
conda install git pip jupyter
```

and then 
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

## Author

- Andreas Vollrath, ESA
