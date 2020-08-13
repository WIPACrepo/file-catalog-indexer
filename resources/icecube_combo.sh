#!/bin/bash
mkdir combo
cd combo
svn co http://code.icecube.wisc.edu/svn/meta-projects/combo/stable/ src
mkdir build
cd build
cmake ../src
make
./env-shell.sh
cd ../..