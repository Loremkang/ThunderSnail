#!/bin/bash

SOURCE_DIR=$(realpath $(dirname "${BASH_SOURCE[0]}"))
pushd . 2>&1 > /dev/null

if [[ $1 == "clean" ]]; then
    rm ${SOURCE_DIR}/build -rf
    popd 2>&1 > /dev/null
    exit
fi

FLAG=release
while [[ $# -gt 0 ]]; do
  case $1 in
    -f|--flag)
      FLAG="$2"
      shift
      shift
      ;;
    -h|--help)
      HELP=true
      shift
      ;;
    *) 
      SRC_FILE=("$1")
      shift
      ;;
  esac
done

if [[ $HELP == true ]]; then
    echo "build script of ThunderSnail"
    echo "    -f, --flag debug     build with debug info"
    echo "    clean                clean build"
    exit
fi

if [[ $FLAG == "debug" ]]; then
    cd ${SOURCE_DIR} && cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
    cd ${SOURCE_DIR}/build && make -j
elif [[ $FLAG == "release" ]]; then 
    cd ${SOURCE_DIR} && cmake -S . -B build
    cd ${SOURCE_DIR}/build && make -j
fi
popd 2>&1 > /dev/null