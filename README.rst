.. image:: https://raw.githubusercontent.com/ESA-PhiLab/OpenSarToolkit/main/docs/source/_images/header_image.PNG
    :alt: cool ost image

Open SAR Toolkit (OST)
======================

.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
    :target: LICENSE
    :alt: License: MIT

.. image:: https://badge.fury.io/py/opensartoolkit.svg
    :target: https://badge.fury.io/py/opensartoolkit
    :alt: PyPI version
    
.. image:: https://img.shields.io/pypi/dm/opensartoolkit?color=307CC2&logo=python&logoColor=gainsboro  
    :target: https://pypi.org/project/opensartoolkit/
    :alt: PyPI - Download

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
   :alt: Black badge
   
.. image:: https://img.shields.io/badge/Conventional%20Commits-1.0.0-yellow.svg
   :target: https://conventionalcommits.org
   :alt: conventional commit

.. image:: https://codecov.io/gh/ESA-PhiLab/OpenSarToolkit/branch/main/graph/badge.svg?token=P32CMJSSA9
    :target: https://codecov.io/gh/ESA-PhiLab/OpenSarToolkit
    :alt: codecov report

.. image:: https://readthedocs.org/projects/opensartoolkit/badge/?version=latest
    :target: https://opensartoolkit.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status
    
.. image:: https://img.shields.io/badge/all_contributors-5-orange.svg
    :target: https://opensartoolkit.readthedocs.io/en/latest/setup/contributors.html
    :alt: all-contributor

Objective
---------

This python package lowers the entry barrier for accessing and pre-processing Sentinel-1 data for land applications and allows users with little knowledge on SAR and python to produce various Analysis-Ready-Data products.

Functionality
-------------

The Open SAR Toolkit (OST) bundles the full workflow for the generation of Analysis-Ready-Data (ARD) of Sentinel-1 for Land in a single high-level python package. It includes functions for data inventory and advanced sorting as well as downloading from various mirrors. The whole pre-processing is bundled in a single function and different types of ARD can be selected, but also customised. OST does include advanced types of ARD such as combined production of calibrated backscatter, interferometric coherence and the dual-polarimetric H-A-Alpha decomposition. Time-series and multi-temporal statistics (i.e. timescans) can be produced for each of these layers and the generation of spatially-seamless large-scale mosaic over time is possible a well.

The Open SAR Toolkit realises this by using an object-oriented approach, providing classes for single scene processing, GRD and SLC batch processing routines. The SAR processing itself relies on ESA's Sentinel-1 Toolbox as well as some geospatial python libraries and the Orfeo Toolbox for mosaicking.

Please refer to our `documentation <https://opensartoolkit.readthedocs.io/en/latest/>`__ to get started.


Examples
--------

Ecuador VV-polarised Timescan Composite
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-   Year: 2016
-   Sensor: Sentinel-1 C-Band SAR.
-   Acquisitions: 6 acquisitions per swath (4 swaths)
-   Output resolution: 30m
-   RGB composite:
    -   Red: VV-maximum
    -   Green: VV-minimum
    -   Blue: VV-Standard deviation

.. image:: https://raw.githubusercontent.com/ESA-PhiLab/OpenSarToolkit/main/docs/source/_images/ecuador_VV_max_min_std.png
    :alt: Ecuador VV-polarised Timescan Composite

Ethiopia VV-VH polarised Timescan Composite
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-   Year: 2016-2017
-   Sensor: Sentinel-1 C-Band SAR.
-   Acquisitions: 7 acquisitions per swath (about 400 scenes over 8 swaths)
-   Output resolution: 30m
-   RGB composite:
    -    Red: VV-minimum
    -    Green: VH-minimum
    -    Blue: VV-Standard deviation

.. image:: https://raw.githubusercontent.com/ESA-PhiLab/OpenSarToolkit/main/docs/source/_images/eth_vvvh_ts.jpeg
    :alt: Ethiopia VV-VH polarised Timescan Composite


Origin of the project
---------------------

Open SAR Toolkit was initially developed at the Food and Agriculture Organization of the United Nations under the `SEPAL <https://github.com/openforis/sepal>`__ project between 2016-2018. It is still available `there <https://github.com/openforis/opensarkit>`__, but has been completely re-factored and transferred into a simpler and less-dependency rich **Python 3** version, which can be found on this page here. 
Instead of using R-Shiny as a GUI, the main interface are now `Jupyter notebooks <https://opensartoolkit.readthedocs.io/en/latest/example/index.html>`__ that are developed in parallel to this core package and should help to get started.

Authors
-------

meet our `contributors <https://opensartoolkit.readthedocs.io/en/latest/setup/contributors.html>`__.
