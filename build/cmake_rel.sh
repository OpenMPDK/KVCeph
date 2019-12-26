#!/bin/bash


sudo env PATH="$PATH" ./_cmake -DWITH_TESTS=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=./ceph-runtime 

