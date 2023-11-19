#
# docker build -f Dockerfile -t atlasai/opensartoolkit:main .
#

FROM ubuntu:20.04

LABEL maintainer="Andreas Vollrath, FAO. Modified Evan Koester, for AtlasAI PBC"
LABEL OpenSARToolkit='0.12.3'

ARG DEBIAN_FRONTEND=noninteractive

ARG OST_USER="ost"
ARG OST_PASSWORD="abc123"

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

ENV OTB_VERSION="7.3.0"
ENV TBX_VERSION="8"
ENV TBX_SUBVERSION="0"
ENV TBX="esa-snap_sentinel_unix_${TBX_VERSION}_${TBX_SUBVERSION}.sh"
ENV SNAP_URL="https://download.esa.int/step/snap/${TBX_VERSION}.${TBX_SUBVERSION}/installers"
ENV OTB=OTB-${OTB_VERSION}-Linux64.run
ENV HOME=/home/${OST_USER}
ENV PATH=$PATH:/home/ost/programs/snap/bin:/home/ost/programs/OTB-${OTB_VERSION}-Linux64/bin

ENV VENV=${HOME}/venv
ENV PIP_NO_CACHE_DIR=1

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y lsb-release software-properties-common && \
    apt-get install -y git wget curl bzip2 unzip gnupg2 sudo

RUN add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.11 python3.11-venv python3.11-dev python3.11-distutils && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1

RUN add-apt-repository -y ppa:ubuntugis/ubuntugis-unstable && \
    apt-get update && \
    apt-get install -y build-essential gdal-bin gdal-data libgdal-dev

RUN apt-get install -y imagemagick

RUN echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update && \
    apt-get -y install postgresql-client-16

RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# add user
RUN useradd -ms /bin/bash ${OST_USER} && \
    usermod -a -G sudo,users ${OST_USER} && \
    bash -c "echo ${OST_USER}:${OST_PASSWORD} | chpasswd" && \
    echo "%sudo ALL=(ALL:ALL) NOPASSWD: ALL" > /etc/sudoers.d/nopasswd

#
# user stuff
#

USER ${OST_USER}

# set work directory to home and download snap
WORKDIR ${HOME}

RUN mkdir -p "$VENV" && \
    python -m venv "$VENV" && \
    echo "source \"$VENV/bin/activate\"" >> ${HOME}/.bashrc

RUN . "$VENV/bin/activate" && \
    pip install "pygdal==$(gdal-config --version).*"

# copy the snap installation config file into the container
COPY snap.varfile $HOME

RUN mkdir -p ${HOME}/programs && \
    wget $SNAP_URL/$TBX && \
    chmod +x $TBX && \
    ./$TBX -q -varfile snap.varfile && \
    rm $TBX && \
    rm snap.varfile && \
    cd ${HOME}/programs && \
    wget https://www.orfeo-toolbox.org/packages/archives/OTB/${OTB} && \ 
    chmod +x $OTB && \
    ./${OTB} && \
    rm -f OTB-${OTB_VERSION}-Linux64.run 

# update snap to latest version
RUN ${HOME}/programs/snap/bin/snap --nosplash --nogui --modules --update-all 2>&1 | while read -r line; do \
        echo "$line" && \
        [ "$line" = "updates=0" ] && sleep 2 && pkill -TERM -f "snap/jre/bin/java"; \
    done; exit 0

# set usable memory to 12G
RUN echo "-Xmx12G" > /home/ost/programs/snap/bin/gpt.vmoptions

# get OST and tutorials
RUN . "$VENV/bin/activate" && \
    pip install git+https://github.com/AtlasAIPBC/OpenSarToolkit.git
