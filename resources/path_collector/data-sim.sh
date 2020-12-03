#!/bin/bash
python3 path_collector_make_condor.py /data/sim --cpus 4 --memory 20G -e /data/sim/sim-new/ /data/sim/scratch/ --chunk-size 1GB --fast-forward --accounting-group 1_week