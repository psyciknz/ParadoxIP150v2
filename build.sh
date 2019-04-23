#!/bin/bash
#
# Run this to build, tag and create fat-manifest for your images


set -e

if [[ -f build.config ]]; then
  source ./build.config
else
  echo ERROR: ./build.config not found.
  exit 1
fi

# Fail on empty params
if [[ -z ${IMAGE_NAME} || -z ${TARGET_ARCHES} ]]; then
  echo ERROR: Please set build parameters.
  exit 1
fi

if [ -z ${REPO} ]; then
   docker login --username ${DOCKERUSER} --password ${DOCKERPASSWORD}
   NEWREPO=''
else 
   NEWREPO="${REPO}/"
   echo $NEWREPO
fi

# Determine OS and Arch.
build_os=$(uname -s | tr '[:upper:]' '[:lower:]' )
build_uname_arch=$(uname -m | tr '[:upper:]' '[:lower:]' )

case ${build_uname_arch} in
  x86_64  ) build_arch=amd64 ;;
  aarch64 ) build_arch=arm ;;
  arm*    ) build_arch=arm ;;
  *)
    echo ERROR: Sorry, unsuppoted architecture ${native_arch};
    exit 1
    ;;
esac

docker_bin_path=$( type -P docker-${build_os}-${build_arch} || type -P ${DOCKER_CLI_PATH%/}/docker-${build_os}-${build_arch} || echo docker-not-found )

if [[ ! -x ${docker_bin_path} ]]; then
  echo ERROR: Missing Docker CLI with manifest command \(docker_bin_path: ${docker_bin_path}\)
  exit 1
fi

if [[ -z ${IMAGE_VERSION} ]]; then
  IMAGE_VERSION="latest"
fi

for docker_arch in ${TARGET_ARCHES}; do
  case ${docker_arch} in
    amd64       ) qemu_arch="x86_64" ;;
    arm32v[5-7] ) qemu_arch="arm" ;;
    arm64v8     ) qemu_arch="aarch64" ;;
    *)
      echo ERROR: Unknown target arch.
      exit 1
  esac
  cp Dockerfile.cross Dockerfile.${docker_arch}
  sed -i  "s|__BASEIMAGE_ARCH__|${docker_arch}|g" Dockerfile.${docker_arch}
  sed -i  "s|__QEMU_ARCH__|${qemu_arch}|g" Dockerfile.${docker_arch}
  if [[ ${docker_arch} == "amd64" || ${build_os} == "darwin" ]]; then
    sed -i  "/__CROSS_/d" Dockerfile.${docker_arch}
  else
    sed -i  "s/__CROSS_//g" Dockerfile.${docker_arch}
  fi
  #${docker_bin_path} build -f Dockerfile.${docker_arch} -t ${REPO}/${IMAGE_NAME}:${docker_arch}-${IMAGE_VERSION} .
  ${docker_bin_path} build -f Dockerfile.${docker_arch} -t ${NEWREPO}${IMAGE_NAME}:${docker_arch}-${IMAGE_VERSION} .
  #${docker_bin_path} push ${NEWREPO}/${IMAGE_NAME}:${docker_arch}-${IMAGE_VERSION}
  ${docker_bin_path} push ${NEWREPO}${IMAGE_NAME}:${docker_arch}-${IMAGE_VERSION}
  #arch_images="${arch_images} ${NEWREPO}/${IMAGE_NAME}:${docker_arch}-${IMAGE_VERSION}"
  arch_images="${arch_images} ${NEWREPO}${IMAGE_NAME}:${docker_arch}-${IMAGE_VERSION}"
  rm Dockerfile.${docker_arch}
done

echo INFO: Creating fat manifest for ${NEWREPO}${IMAGE_NAME}:${IMAGE_VERSION}
echo INFO: with subimages: ${arch_images}
if [ -d ${HOME}/.docker/manifests/docker.io_${NEWREPO}_${IMAGE_NAME}-${IMAGE_VERSION} ]; then
  rm -rf ${HOME}/.docker/manifests/docker.io_${NEWREPO}_${IMAGE_NAME}-${IMAGE_VERSION}
fi
docker manifest create --amend ${NEWREPO}${IMAGE_NAME}:${IMAGE_VERSION} ${arch_images}
for docker_arch in ${TARGET_ARCHES}; do
  case ${docker_arch} in
    amd64       ) annotate_flags="" ;;
    arm32v[5-7] ) annotate_flags="--os linux --arch arm" ;;
    arm64v8     ) annotate_flags="--os linux --arch arm64 --variant armv8" ;;
  esac
  echo INFO: Annotating arch: ${docker_arch} with \"${annotate_flags}\"
  docker manifest annotate ${NEWREPO}${IMAGE_NAME}:${IMAGE_VERSION} ${NEWREPO}${IMAGE_NAME}:${docker_arch}-${IMAGE_VERSION} ${annotate_flags}
done
echo INFO: Pushing ${NEWREPO}${IMAGE_NAME}:${IMAGE_VERSION}
docker manifest push ${NEWREPO}${IMAGE_NAME}:${IMAGE_VERSION}
