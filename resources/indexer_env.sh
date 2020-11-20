#!/bin/bash
eval `/cvmfs/icecube.opensciencegrid.org/py3-v4.1.0/setup.sh`
python3 -m virtualenv -p python3 env-fc-indexer
. env-fc-indexer/bin/activate
pip install -r requirements.txt
$SROOT/metaprojects/combo/stable/env-shell.sh $@
