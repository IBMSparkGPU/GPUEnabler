#!/bin/bash

CUDA=0
CUDAVER=0
#
# Identify the CUDA runtime version to activate the correct profile
# Install Cuda for your platform from
# https://developer.nvidia.com/cuda-downloads.
# 
type nvcc >/dev/null 2>&1 && CUDA=1

if [[ $CUDA != 0 ]]; then
  CUDAVER=`nvcc --version | tail -1 | awk '{ print \$5 }' | cut -d ',' -f 1`
  if [[ $CUDAVER == "7.5" ]]; then
    echo "Identified CUDA version is 7.5"
    CUDAVER="jcuda75"
  elif [[ $CUDAVER == "7.0" ]]; then
    echo "Identified CUDA version is 7.0a"
    CUDAVER="jcuda70a"
  else
    echo "Not a supported version. Installation will fallback to default"
  fi
fi

MVN_CMD="mvn"

if [[ $CUDAVER != 0 ]]; then
  MVN_ARGS="-P$CUDAVER -Dmaven.compiler.showWarnings=true -Dmaven.compiler.showDeprecation=true"
else
  MVN_ARGS="-Dmaven.compiler.showWarnings=true -Dmaven.compiler.showDeprecation=true"
fi

echo "Executing :: $MVN_CMD $MVN_ARGS -DskipTests clean install"

$MVN_CMD $MVN_ARGS -DskipTests clean install $@ 2>&1 | tee ~/compile.txt

# ./utils/embed.sh -d  gpu-enabler_2.11-1.0.0.jar

