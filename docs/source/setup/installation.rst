Installation
============

In this section the different ways of installing OST are presented. 

Docker
------

.. danger::

    Dockerhub is not permitting automatic builds. Therefore you need to build your own docker image using the `DOCKERFIlE <https://raw.githubusercontent.com/ESA-PhiLab/OpenSarToolkit/main/Dockerfile>`__.
    The resulting docker image contains the full package, including ESA's Sentinel-1 Toolbox, Orfeo Toolbox, Jupyter Lab as well as the Open SAR Toolkit tutorial notebooks.

Docker installation is possible on various OS. Installation instructions can be found at https://docs.docker.com/install/

After docker is installed and running, launch the container with 
(adapt the path to the shared host folder and the name of the docke rimage at the very end):

.. code-block:: console

    docker run -it -p 8888:8888 -v /shared/folder/on/host:/home/ost/shared docker/image

The docker image automatically executes the jupyter lab and runs it on port 8888. You can find the address to the notebook on the command line where docker is running. Copy it into your favorites browser and replace 127.0.0.1 with localhost.

Manual installation
-------------------

Dependencies
^^^^^^^^^^^^

Sentinel Application Toolbox (SNAP)
"""""""""""""""""""""""""""""""""""

OST bases mainly on the freely available SNAP toolbox for the SAR-specific processing routines. You can download SNAP from: http://step.esa.int/main/download/

If you install SNAP into the standard directory, OST should have no problems to find the SNAP command line executable. Otherwise you need to define the path to the gpt file on your own during processing.

**Make sure to use SNAP 8 with the latest updates installed.**

Orfeo Toolbox
"""""""""""""

If you want to create mosaics between different swaths, OST will rely on the :code:`otbcli_Mosaic` command from The Orfeo Toolbox. You can download Orfeo from: https://www.orfeo-toolbox.org/download/

Make sure that the Orfeo bin folder is within your PATH variable to allow execution from command line.

Further dependencies (libs etc)
"""""""""""""""""""""""""""""""

Ubuntu 18.04 and later:

.. code-block:: console

    sudo apt install python3-pip git libgdal-dev python3-gdal libspatialindex-dev nodejs npm libgfortran5

Any Operating system using (mini)conda https://www.anaconda.com/:

.. code-block:: console

    conda install pip gdal jupyter jupyterlab git matplotlib numpy rasterio imageio rtree geopandas fiona shapely matplotlib descartes tqdm scipy joblib retrying pytest pytest-cov nodejs

OST installation
^^^^^^^^^^^^^^^^

You can then use pip to install Open SAR Toolkit: 

.. code-block:: console

    pip install opensartoolkit