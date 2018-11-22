FROM ubuntu:bionic

MAINTAINER Andreas Vollrath 

# install python dependencies and wget
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -yq \
    python3 \
    python3-geopandas \
    python3-psycopg2 \
    python3-rtree \
    python3-rasterio \
    python3-shapely \
    python3-fiona \
    python3-requests \
    python3-tqdm \
    python3-gdal \
    wget

# add the OST python package to the site packages
ADD . /usr/lib/python3/dist-packages/ost/

# install dependencies 
RUN sh /usr/lib/python3/dist-packages/ost/bin/install.sh

# create custom gpt file with dynamic heap size and tile cache
RUN bash /usr/lib/python3/dist-packages/ost/bin/createGpt.sh /usr/local/snap/ /usr/bin/gpt && chmod +x /usr/bin/gpt
