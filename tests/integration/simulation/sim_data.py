"""Example file metadata."""

from typing import Any, Dict

EXAMPLES: Dict[str, Dict[str, Any]] = {
    "L2/Level2_IC86.2011_corsika.010285.000000.i3.bz2": {
        "checksum": {
            "sha512": "f246b07132968374ded4627b2fc93c8af5a77468ced5b90327fe8ac00685e6ff549a61daac37a3fbbdf51a1a8add35cb10ed8d402ef89eaf1ed4c272da150331"
        },
        "content_status": "good",
        "create_date": "2010-02-01",
        "data_type": "simulation",
        "file_size": 30,
        "iceprod": {
            "config": "https://grid.icecube.wisc.edu/simulation/dataset/10285",
            "dataset": 10285,
            "dataset_id": "10285",
            "job": 0,
            "job_id": "0",
            "task": None,
            "task_id": None,
        },
        "simulation": {
            "GCD_file": "GeoCalibDetectorStatus_IC86.55697_corrected_V2.i3.gz",
            "atmosphere": -1,
            "bulk_ice_model": "SPICEMie",
            "composition": "5-component model",
            "cylinder_length": 1600.0,
            "cylinder_radius": 800.0,
            "energy_max": 100000.0,
            "energy_min": 600.0,
            "generator": "CORSIKA",
            "geometry": "IC86.2011",
            "n_events": 10000000,
            "oversampling": 1,
            "photon_propagator": "ClSim",
            "power_law_index": "E^-2.6",
        },
        "locations": [
            {
                "site": "WIPAC",
                "path": "/data/sim/IceCube/2011/filtered/level2/CORSIKA-in-ice/10285/00000-00999/Level2_IC86.2011_corsika.010285.000000.i3.bz2",
            }
        ],
        "logical_name": "/data/sim/IceCube/2011/filtered/level2/CORSIKA-in-ice/10285/00000-00999/Level2_IC86.2011_corsika.010285.000000.i3.bz2",
        "processing_level": "L2",
    },
    "Corsika_IP1/TopSimulator_IC86_corsika_icetop.010410.000001.i3.bz2": {
        "checksum": {
            "sha512": "d54f662f21df9bfa5e697731ebc31aeff7c7aa77fc929fb21cf1d8ee027e4b89bb6540b2f4829abcb3466e233b39e066bb90927b97d490076ec986ce96f2e942"
        },
        "content_status": "good",
        "create_date": "2010-02-01",
        "data_type": "simulation",
        "file_size": 35,
        "iceprod": {
            "config": "https://grid.icecube.wisc.edu/simulation/dataset/10410",
            "dataset": 10410,
            "dataset_id": "10410",
            "job": 1,
            "job_id": "1",
            "task": None,
            "task_id": None,
        },
        "simulation": {
            "atmosphere": 13,
            "energy_max": 100000000.0,
            "energy_min": 100000.0,
            "n_events": 1,
            "power_law_index": "E^-1",
        },
        "locations": [
            {
                "site": "WIPAC",
                "path": "/data/sim/IceCube/2011/generated/CORSIKA-ice-top/12333/topsimulator/0000000-0000999/TopSimulator_IC86_corsika_icetop.010410.000001.i3.bz2",
            }
        ],
        "logical_name": "/data/sim/IceCube/2011/generated/CORSIKA-ice-top/12333/topsimulator/0000000-0000999/TopSimulator_IC86_corsika_icetop.010410.000001.i3.bz2",
        "processing_level": "Generated",
    },
    "Corsika_IP2/corsika.020900.000982.i3.zst": {
        "checksum": {
            "sha512": "d54f662f21df9bfa5e697731ebc31aeff7c7aa77fc929fb21cf1d8ee027e4b89bb6540b2f4829abcb3466e233b39e066bb90927b97d490076ec986ce96f2e942"
        },
        "content_status": "good",
        "create_date": "2010-02-01",
        "data_type": "simulation",
        "file_size": 35,
        "iceprod": {
            "config": "https://iceprod2.icecube.wisc.edu/config?dataset_id=272bbc78e48a11e98ae4141877284d92",
            "dataset": 20900,
            "dataset_id": "272bbc78e48a11e98ae4141877284d92",
            "job": 982,
            "job_id": "8a9bc842e48b11e98ae4141877284d92",
            "task": "generate",
            "task_id": "8a9cc2b0e48b11e98ae4141877284d92",
        },
        "simulation": {
            "GCD_file": "/cvmfs/icecube.opensciencegrid.org/data/GCD/GeoCalibDetectorStatus_AVG_55697-57531_PASS2_SPE_withStdNoise.i3.gz",
            "energy_max": 100000000.0,
            "energy_min": 600.0,
            "generator": "CORSIKA-in-ice",
            "geometry": "IC86.2016",
            "hadronic_interaction": "Sybill-2.3",
            "n_events": 100000,
            "power_law_index": "E^-2",
        },
        "locations": [
            {
                "site": "WIPAC",
                "path": "/data/sim/IceCube/2016/generated/CORSIKA-in-ice/20900/0000000-0000999/corsika.020900.000982.i3.zst",
            }
        ],
        "logical_name": "/data/sim/IceCube/2016/generated/CORSIKA-in-ice/20900/0000000-0000999/corsika.020900.000982.i3.zst",
        "processing_level": "Generated",
    },
    "Triggered/Detector_IC86_corsika_icetop.010410.000001.i3.bz2": {
        "checksum": {
            "sha512": "24cc25ee89f8a3669b4f48be58e806ab9ce01fd185a991a2888a464fa9f6c36cda2eb0fdcf0f6213ae68b54008cf756159c443cc96b1c55ca9cac1142e260093"
        },
        "content_status": "good",
        "create_date": "2010-02-01",
        "data_type": "simulation",
        "file_size": 34,
        "iceprod": {
            "config": "https://grid.icecube.wisc.edu/simulation/dataset/10410",
            "dataset": 10410,
            "dataset_id": "10410",
            "job": 1,
            "job_id": "1",
            "task": None,
            "task_id": None,
        },
        "simulation": {
            "atmosphere": 13,
            "energy_max": 100000000.0,
            "energy_min": 100000.0,
            "n_events": 1,
            "power_law_index": "E^-1",
        },
        "locations": [
            {
                "site": "WIPAC",
                "path": "/data/sim/IceCube/2011/generated/CORSIKA-ice-top/12333/detector/0000000-0000999/Detector_IC86_corsika_icetop.010410.000001.i3.bz2",
            }
        ],
        "logical_name": "/data/sim/IceCube/2011/generated/CORSIKA-ice-top/12333/detector/0000000-0000999/Detector_IC86_corsika_icetop.010410.000001.i3.bz2",
        "processing_level": "Triggered",
    },
}
