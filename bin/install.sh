#!/bin/bash

# install orfeo
otb=OTB-6.6.0-Linux64
wget https://www.orfeo-toolbox.org/packages/$otb.run 
chmod +x $otb.run 
mv $otb.run /usr/local/lib
cd /usr/local/lib 
./$otb.run 
rm $otb.run
echo "export PATH=${PATH}:/usr/local/lib/$otb/bin" >> ~/.bashrc
cd -

# install snap
wget http://step.esa.int/downloads/5.0/installers/esa-snap_sentinel_unix_5_0.sh
sh esa-snap_sentinel_unix_5_0.sh -q -overwrite && rm -f esa-snap_sentinel_unix_5_0.sh
snap --nosplash --nogui --modules --update-all
