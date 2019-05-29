FROM ubuntu:18.04


LABEL maintainer="Andreas Vollrath, ESA phi-lab"
LABEL OpenSARToolkit='0.1'

RUN groupadd -r ost \
    && useradd -r -g ost ost\
    && mkdir /home/ost
    
    
# install python dependencies and wget
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -yq \
    python3 \
    python3-pip \
    git \
    libgdal-dev \
    python3-gdal \
    libspatialindex-dev \
    wget \
    && rm -fr /var/lib/apt/lists/*

# update variables
ENV \ 
  TBX="esa-snap_sentinel_unix_6_0.sh" \
  SNAP_URL="http://step.esa.int/downloads/6.0/installers" \
  HOME=/home/ost

# set work directory to home and download snap
WORKDIR /home/ost

RUN wget $SNAP_URL/$TBX \ 
  && chmod +x $TBX
   
# get OST
RUN python3 -m pip install git+https://github.com/ESA-PhiLab/OpenSarToolkit.git

# install jupyter lab
RUN python3 -m pip install jupyterlab

COPY snap6.varfile $HOME

RUN ./$TBX -q -varfile snap6.varfile \
  && rm $TBX \
  && rm snap6.varfile

# ENV PATH=$PATH:/home/worker/snap/bin \
#          SNAP_PATH=/home/worker/snap/bin

#RUN /home/ost/snap/bin/snap --nosplash --nogui --modules --list --refresh
#RUN /home/ost/snap/bin/snap --nosplash --nogui --modules --update-all

EXPOSE 8888
CMD jupyter lab --ip='0.0.0.0' --port=8888 --no-browser --allow-root