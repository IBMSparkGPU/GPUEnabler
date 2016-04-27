#!/bin/bash
SCRIPT_DIR=$(cd $(dirname $(readlink -f $0 || echo $0));pwd -P)

DIR=$SCRIPT_DIR/..
LIBPREFIX="lib"

#ABSOLUTE_DIR=`realpath $DIR`
ABSOLUTE_DIR=`readlink -e $DIR`

usage() {
  echo "Usage: $0 [-a ARCH] [-o OS] [-e EXTENSION] [-d JARFILE] " 1>&2
  exit 1
}


if [ 'uname -s' == "Darwin" ]; then
  OS=apple
  ARCH=x86_64
  EXT=dylib
else
  OS=linux
  ARCH=`arch`
  EXT=so
fi

if [ $ARCH == "ppc64le" ]; then
  ARCH="ppc_64"
fi

function addlib2jar {
  libdir=gpu-enabler/target
  lib=lib/$2-${OS}-${ARCH}.${EXT}
  if [ ! -f "$libdir/$lib" ]; then
    echo "file $libdir/$lib does not exist. Please check file name." >&2
    return 255
  fi
  jar=$libdir/$1
  if [ -e $libdir/$lib ]; then
    if [ -e $jar ]; then
      echo "$jar is being processed" >& 2
      jar uf $jar -C $libdir $lib && echo "  added $lib" >&2
    else
      echo "file $jar does not exist. Please check file name." >&2
      return 255
    fi 
  fi
  return 1 
}

while getopts a:o:e:d:v:h OPT
do
  case $OPT in
    a) ARCH=$OPTARG
       ;;
    o) OS=$OPTARG
       ;;
    e) EXT=$OPTARG
       ;;
    d) JARFILE=$OPTARG
       ;;
    h) usage
       ;;
  esac
done
shift $((OPTIND - 1))

if [ ! -z $JARFILE ] ; then 
    addlib2jar $JARFILE libJCudaDriver
    # addlib2jar $JARFILE JCudaDriver-native 
    if [ $? == 1 ]; then
      addlib2jar $JARFILE libJCudaRuntime
      # addlib2jar $JARFILE JCudaRuntime-native
      if [ $? == 1 ]; then
        exit 0
      fi
    fi
    echo "Encounter error while patching"
    exit 255
else 
    echo "JARFILE is unset"; 
fi

