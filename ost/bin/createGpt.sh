#!/bin/bash

snap_installation_path=$1
gptpath=$2

echo '#!/bin/bash' > ${gptpath}
echo "" >> ${gptpath}

unamestr=`uname`
if [[ "$unamestr" == 'Linux' ]]; then
    echo "TOT_MEM=`free -m | awk 'NR==2' | awk '{print $2}'`" >> ${gptpath}
elif [[ "$unamestr" == 'Darwin' ]]; then
    echo "MEM_in_GB=`system_profiler SPHardwareDataType | grep Memory | awk '{print $2}'`" >> ${gptpath}
    echo 'TOT_MEM=`echo $MEM_in_GB | bc -l`' >> ${gptpath}
fi

echo 'HEAP_MEM=$(expr $TOT_MEM \* 3 \/ 4)' >> ${gptpath}
echo 'TILE_CACHE=$(expr $HEAP_MEM \* 2 \/ 3)' >> ${gptpath}
echo ""
echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH:. ${snap_installation_path}/jre/bin/java \\" >> ${gptpath}
echo ' -Djava.awt.headless=true\' >> ${gptpath}
echo ' -Dsnap.mainClass=org.esa.snap.core.gpf.main.GPT\' >> ${gptpath}
echo " -Dsnap.home=/usr/local/snap\\" >> ${gptpath}
echo ' -Dsnap.jai.tileCacheSize=${TILE_CACHE}m\' >> ${gptpath}
echo ' -Dsnap.log.level=ERROR\' >> ${gptpath}
echo ' -Xmx${HEAP_MEM}m\' >> ${gptpath}
echo ' -Xms256m\' >> ${gptpath}
echo ' -XX:+AggressiveOpts\' >> ${gptpath}
echo ' -Xverify:none\' >> ${gptpath}
echo " -jar ${snap_installation_path}/snap/modules/ext/org.esa.snap.snap-core/org-esa-snap/snap-runtime.jar \$@" >> ${gptpath}

