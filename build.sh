#!/bin/bash

SOURCE_DIR=$(realpath $(dirname "${BASH_SOURCE[0]}"))
pushd . 2>&1 > /dev/null

if [[ $1 == "clean" ]]; then
    rm ${SOURCE_DIR}/build -rf
else
    cd ${SOURCE_DIR} && cmake -S . -B build 
    cd ${SOURCE_DIR}/build && make -j
fi

popd 2>&1 > /dev/null