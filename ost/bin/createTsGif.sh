#!/bin/bash

function s1_create_thumb_rgb() {

	if [ -z "$4" ]; then
	    echo " Missing arguments. Syntax:"
	    echo " s1_create_thumb_rgb <input_VV> <input_VH> <outfile> <tmp-folder>"
	    return
  fi

  INPUT_VV=$(readlink -f $1)
  INPUT_VH=$(readlink -f $2)
  OUT_TN=$(readlink -f $3)
  TMP_TN=$(readlink -f $4)

  INPUT_VV_BASE=$(basename ${INPUT_VV})
  INPUT_VH_BASE=$(basename ${INPUT_VH})

  source ${OPENSARKIT}/lib/helpers_source
	echo -ne " Creating a thumbnail file for the acquisition of ${DATE} ..."
  # resize and  bring to 8 bit the VV channel
  gdal_translate -outsize 20% 20% -a_nodata 0 ${INPUT_VV} ${TMP_TN}/${INPUT_VV_BASE}
  gdal_contrast_stretch -ndv 0 -percentile-range 0.01 0.99 ${TMP_TN}/${INPUT_VV_BASE} ${TMP_TN}/${INPUT_VV_BASE}.8bit.tif
  rm -f ${TMP_TN}/${INPUT_VV_BASE}

  # resize and  bring to 8 bit the VH channel
  gdal_translate -outsize 20% 20% -a_nodata 0 ${INPUT_VH} ${TMP_TN}/${INPUT_VH_BASE}
  gdal_contrast_stretch -ndv 0 -percentile-range 0.01 0.99 ${TMP_TN}/${INPUT_VH_BASE} ${TMP_TN}/${INPUT_VH_BASE}.8bit.tif
  rm -f ${TMP_TN}/${INPUT_VH_BASE}

  # create ratio band in float and stretch to 8 bit
  gdal_calc.py --overwrite -A ${TMP_TN}/${INPUT_VV_BASE}.8bit.tif -B ${TMP_TN}/${INPUT_VH_BASE}.8bit.tif --calc="A/B" --type=Float32 --outfile=${TMP_TN}/${INPUT_VV_BASE}.VVVH.TN.tif
  gdal_contrast_stretch -ndv 0 -percentile-range 0.02 0.98 ${TMP_TN}/${INPUT_VV_BASE}.VVVH.TN.tif ${TMP_TN}/${INPUT_VV_BASE}.VVVH.TN.8bit.tif
  rm -f ${TMP_TN}/${INPUT_VV_BASE}.VVVH.TN.tif

  # create RGB Thumbnail
  gdal_merge.py -separate -co "COMPRESS=LZW" -a_nodata 0 -o ${OUT_TN} \
                      ${TMP_TN}/${INPUT_VV_BASE}.8bit.tif ${TMP_TN}/${INPUT_VH_BASE}.8bit.tif ${TMP_TN}/${INPUT_VV_BASE}.VVVH.TN.8bit.tif
  duration=$SECONDS && echo -e " done ($(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed)"

}

function s1_create_labeled_jpeg() {

	if [ -z "$3" ]; then
	    echo " Missing arguments. Syntax:"
	    echo "  s1_create_labeled_jpeg <input> <label> <output.jpg>"
	    return
  fi

  # create JPEG for gif creation
  gdal_translate -of JPEG -a_nodata 0 $1 $3

  # add the acq date to jpeg
  WIDTH_TN=$(identify -format %w $3)
  HEIGHT_TN=$(identify -format %h $3)
  HEIGHT_LABEL=$(expr $HEIGHT_TN / 15)

  # add annotation with date
  convert -background '#0008' -fill white -gravity center -size ${WIDTH_TN}x${HEIGHT_LABEL} caption:"$2" \
         $3 +swap -gravity north -composite $3

}

TS_FOLDER=$(readlink -f $1)
cd $TS_FOLDER
mkdir -p TMP
TMP=$TS_FOLDER/TMP
 
for file in $(ls -1 *VV*tif);do
    ls $file
    date=`echo $file | awk -F '.' '{print $2}'`
    echo $date
    fileVH=`echo $file | sed -e 's/VV/VH/g'`
    echo $fileVH
    s1_create_thumb_rgb $file $fileVH $file.tn.tif $TMP
    s1_create_labeled_jpeg $file.tn.tif $date $file.jpg
done

 convert -delay 200 -loop 20 $(ls -1 *.jpg | sort -g) ${TS_FOLDER}/time_animation.gif


