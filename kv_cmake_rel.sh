#!/bin/bash


./do_cmake_release.sh -DWITH_TESTS=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=./ceph-runtime ..

