#!/bin/bash
eval `/cvmfs/icecube.opensciencegrid.org/py3-v4.1.0/setup.sh`
python3 -m virtualenv -p python3 /home/eevans/env-fc-indexer
. /home/eevans/env-fc-indexer/bin/activate
pip install -r /tmp/requirements.txt
$SROOT/metaprojects/combo/stable/env-shell.sh $@
