#!/bin/bash
mkdir combo
cd combo
svn co http://code.icecube.wisc.edu/svn/meta-projects/combo/stable/ src --username=icecube --password=skua --no-auth-cache
mkdir build
cd build
cmake ../src
make
cd ../..
./combo/build/env-shell.sh