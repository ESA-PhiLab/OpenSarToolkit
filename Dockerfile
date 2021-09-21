FROM ubuntu:20.04

LABEL maintainer="Andreas Vollrath, FAO"
LABEL OpenSARToolkit='0.12.3'

# set work directory to home and download snap
WORKDIR /home/ost

# copy the snap installation config file into the container
COPY snap.varfile $HOME

# update variables
ENV OTB_VERSION="7.3.0" \
    TBX_VERSION="8" \
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
        unzip \
        imagemagick \
        nodejs \
        npm

RUN alias python=python3 && \
    rm -rf /var/lib/apt/lists/*  && \
    python3 -m pip install jupyterlab && \
    mkdir /home/ost/programs && \
    wget $SNAP_URL/$TBX && \    
    chmod +x $TBX && \
    ./$TBX -q -varfile snap.varfile && \
    rm $TBX && \
    rm snap.varfile && \
    cd /home/ost/programs && \
    wget https://www.orfeo-toolbox.org/packages/${OTB} && \ 
    chmod +x $OTB && \
    ./${OTB} && \
    rm -f OTB-${OTB_VERSION}-Linux64.run 

# update snap to latest version
RUN /home/ost/programs/snap/bin/snap --nosplash --nogui --modules --update-all 2>&1 | while read -r line; do \
        echo "$line" && \
        [ "$line" = "updates=0" ] && sleep 2 && pkill -TERM -f "snap/jre/bin/java"; \
    done; exit 0

# set usable memory to 12G
RUN echo "-Xmx12G" > /home/ost/programs/snap/bin/gpt.vmoptions

# get OST and tutorials
RUN python3 -m pip install git+https://github.com/ESA-PhiLab/OpenSarToolkit.git && \
    git clone https://github.com/ESA-PhiLab/OST_Notebooks && \
    jupyter labextension install @jupyter-widgets/jupyterlab-manager && \
    jupyter nbextension enable --py widgetsnbextension

EXPOSE 8888
CMD jupyter lab --ip='0.0.0.0' --port=8888 --no-browser --allow-root
