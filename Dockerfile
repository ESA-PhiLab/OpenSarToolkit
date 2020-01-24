FROM ubuntu:18.04

LABEL maintainer="Petr Sevcik, EOX IT Services GmbH"
LABEL OpenSARToolkit='0.8.1'

# set work directory to home and download snap
WORKDIR /home/ost

# copy the snap installation config file into the container
COPY snap7.varfile $HOME

# update variables
ENV OTB_VERSION="7.0.0" \
    TBX_VERSION="7" \
    TBX_SUBVERSION="0"
ENV TBX="esa-snap_sentinel_unix_${TBX_VERSION}_${TBX_SUBVERSION}.sh" \
  SNAP_URL="http://step.esa.int/downloads/${TBX_VERSION}.${TBX_SUBVERSION}/installers" \
  OTB=OTB-${OTB_VERSION}-Linux64.run \
  HOME=/home/ost \
  PATH=$PATH:/home/ost/programs/snap/bin:/home/ost/programs/OTB-${OTB_VERSION}-Linux64/bin
   
# install all dependencies
RUN groupadd -r ost && \
    useradd -r -g ost ost && \
    apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -yq \
        python3 \
        python3-pip \
        git \
        libgdal-dev \
        python3-gdal \
        libspatialindex-dev \
        libgfortran3 \
        wget \
        nodejs \
        npm && \
    rm -rf /var/lib/apt/lists/*  && \
    python3 -m pip install jupyterlab && \
    mkdir /home/ost/programs && \
    wget $SNAP_URL/$TBX && \    
    chmod +x $TBX && \
    ./$TBX -q -varfile snap7.varfile && \
    rm $TBX && \
    rm snap7.varfile && \
    cd /home/ost/programs && \
    wget https://www.orfeo-toolbox.org/packages/${OTB} && \ 
    chmod +x $OTB && \
    ./${OTB} && \
    rm -f OTB-${OTB_VERSION}-Linux64.run 

#RUN /home/ost/snap/bin/snap --nosplash --nogui --modules --list --refresh
#RUN /home/ost/snap/bin/snap --nosplash --nogui --modules --update-all

# get OST and tutorials
RUN git clone https://github.com/ESA-PhiLab/OpenSarToolkit.git && \
    python3 -m pip install -r requirements.txt && \
    python3 setup.py install && \
    git clone https://github.com/ESA-PhiLab/OST_Notebooks

#ENV SHELL="/bin/bash/"
EXPOSE 8888
CMD jupyter lab --ip='0.0.0.0' --port=8888 --no-browser --allow-root