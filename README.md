# Open SAR Toolkit (OST)

## A note on its origin

Open SAR Toolkit was initially developed at the Food and Agriculture Organization of the United Nations under the SEPAL project (https://github.com/openforis/sepal) between 2016-2018. It is still available there (https://github.com/openforis/opensarkit), but has been completely re-factored and transferred into a simpler and less-dependency rich python3 version, which can be found on this page here. Instead of using R-Shiny as a GUI, the main interface are now Jupyter notebooks that are developed in parallel to this core package and should help to get started.

## Objective

Compared to its optical counterpart, the community of Synthetic Aperture Radar (SAR) data users for land applications is still small. One major reason for that originates from the differences in the acquisition principle and the underlying physics of the imaging process. For non-experts, this results in difficulties of applying the correct processing steps as well as interpreting the non-intuitive backscatter image composites. On the other hand, the free and open access to Sentinel-1 data widened the community of interested users and paves the way for the integration of SAR data into operational monitoring systems.

This python package lowers the entry barrier for pre-processing Sentinel-1 data and allows users with little knowledge on SAR and python to produce analysis-ready small to large-scale SAR datasets. OST includes fully automated routines that are mainly build on top of the Sentinel Application Platform (SNAP) and other freely available open-source software such as GDAL, Orfeo Toolbox and Python.

## Functionality

For the moment, Sentinel-1 data inventory and download routines, as well as a GRD to RTC processor allows for the rapid generation of radiometrically terrain corrected (RTC) imagery that is ready for subsequent analysis tasks such as land cover classification. More advanced and processing intensive data products, such as time-series and timescan imagery can be easily produced as well in a fully automatic manner. Ultimately, mosaicking generates seamless wide-area data sets.

## Installation

OST is developed under Ubuntu 18.04 OS in python 3.6. It has not been tested much on other OS and python versions,
but should in principle work on any OS and any python version >= 3.5.

You can install in your global site packages like this:

```
pip3 install git+git://github.com/ESA-PhiLab/OpenSarToolkit
```

or in your local home folder like this:

```
pip3 install git+git://github.com/ESA-PhiLab/OpenSarToolkit
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
