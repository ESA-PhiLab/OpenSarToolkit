#!/bin/bash

set -e

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done
SCRIPTDIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

cd $SCRIPTDIR

DOCKER_REGISTRIES=()
while getopts ":i:r:d" opt; do
	case "${opt}" in
		i)
			IMAGE_VERSION=${OPTARG}
			;;
		r)
			DOCKER_REGISTRIES[${#DOCKER_REGISTRIES[@]}]=${OPTARG}
			;;
		d)
			DEPLOY_ONLY=1
			;;
		:)
			echo "Usage: $0 -i IMAGE_VERSION [-r GCR_REGISTRY] [-d]" >&2
			exit 1
			;;
	esac
done
shift $((OPTIND - 1))

if [ -z "IMAGE_VERSION" ]; then
	echo "Usage: $0 -i IMAGE_VERSION [-r GCR_REGISTRY] [-d]" >&2
	exit 1
fi

TAG="$IMAGE_VERSION"

#
# docker image
#

DOCKER_IMAGE="atlasai/opensartoolkit:$TAG"
if [ -z "$DEPLOY_ONLY" ]; then
	echo
	echo "Building docker image: ${DOCKER_IMAGE}"
	docker build $@ --rm -f Dockerfile -t "$DOCKER_IMAGE" .
fi

#
# push to registry
#

if [ ${#DOCKER_REGISTRIES[@]} -lt 1 ]; then
	echo "No Docker Registry set. All done!"
	exit 0
fi

echo "${DOCKER_REGISTRIES[*]}" | grep "docker\.pkg\.dev"
if [ "$?" == "0" ]; then
	echo
	echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
	echo
	echo "Make sure that glcoud is set up as the credential helper: gcloud auth configure-docker <REGION>-docker.pkg.dev"
	echo
	echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
fi

echo

for registry in "${DOCKER_REGISTRIES[@]}"; do
	REMOTE_DOCKER_TAG="${registry}/base:${TAG}"

	echo
	echo "Tagging remote docker image: \"${DOCKER_IMAGE}\" => \"${REMOTE_DOCKER_TAG}\""
	docker tag "${DOCKER_IMAGE}" "${REMOTE_DOCKER_TAG}"

	echo
	echo "Pushing remote docker image: \"${REMOTE_DOCKER_TAG}\""
	docker push "${REMOTE_DOCKER_TAG}"

	echo
	echo "Docker images pushed to Docker Registry: ${registry}"
done

echo
echo "All done!"
