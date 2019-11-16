#!/bin/bash
sudo env PATH="$PATH" ./kvceph-conf/_cmake -DCMAKE_BUILD_TYPE=Debug -DWITH_TESTS=ON -DCMAKE_INSTALL_PREFIX=./ceph-runtime ..

