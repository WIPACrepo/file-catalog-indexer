"""Example IceProd dataset steering parameter dicts."""


from typing import List, TypedDict

from file_catalog.schema import types
from indexer.metadata.simulation import iceprod_tools


class _TestingExampleSet(TypedDict):
    steering_parameters: iceprod_tools.SteeringParameters
    simulation_metadata: types.SimulationMetadata


EXMAPLES: List[_TestingExampleSet] = [
    # no keys
    {"steering_parameters": {}, "simulation_metadata": {}},
    # extra key
    {"steering_parameters": {"extra-key": 5}, "simulation_metadata": {}},
    # verbatim key
    {
        "steering_parameters": {"cylinder_radius": 5},
        "simulation_metadata": {"cylinder_radius": 5},
    },
    # case-insensitive key
    {
        "steering_parameters": {"RADIUS": 5},
        "simulation_metadata": {"cylinder_radius": 5},
    },
    # prefixed keys
    {
        "steering_parameters": {"MMC::length": 5, "CORSIKA::length": 9},
        "simulation_metadata": {"cylinder_length": 5},
    },
    # type-casting values
    {
        "steering_parameters": {"oversize": "77"},
        "simulation_metadata": {"DOMoversize": 77},
    },
    # bad but accepted type-casting values
    {
        "steering_parameters": {"oversize": "seventy-seven"},
        "simulation_metadata": {"DOMoversize": "seventy-seven"},  # type: ignore[typeddict-item]
    },
    {
        "steering_parameters": {"oversize": ["seventy-seven", "list"]},  # type: ignore[dict-item]
        "simulation_metadata": {"DOMoversize": ["seventy-seven", "list"]},  # type: ignore[typeddict-item]
    },
    # multiple matches: order of precedence
    {
        "steering_parameters": {
            "GENERATION::n_events": 99,
            "nevents": 55,  # chosen b/c it's the shortest, then alphabetically earliest
            "NumberOfPrimaries": 66,
            "showers": 44,  # not chosen b/c it's alphabetically after "nevents"
            "n_events": 77,
        },
        "simulation_metadata": {"n_events": 55},
    },
    # Special Cases: generator key(s)
    {
        "steering_parameters": {"category": "FOO"},
        "simulation_metadata": {"generator": "FOO"},
    },
    {
        "steering_parameters": {"mctype": "FOO"},
        "simulation_metadata": {"generator": "FOO"},
    },
    {
        "steering_parameters": {"mctype": "BAR", "category": "FOO"},
        "simulation_metadata": {"generator": "FOO"},
    },
    {
        "steering_parameters": {"mctype": "BAR", "category": "FOO", "generator": "BAZ"},
        "simulation_metadata": {"generator": "FOO"},
    },
    # Special Cases: power_law_index key
    {
        "steering_parameters": {"power_law_index": "E^-9"},
        "simulation_metadata": {"power_law_index": "E^-9"},
    },
    {
        "steering_parameters": {"power_law_index": "-9"},
        "simulation_metadata": {"power_law_index": "E^-9"},
    },
    {
        "steering_parameters": {"power_law_index": "9"},
        "simulation_metadata": {"power_law_index": "E^-9"},
    },
    {
        "steering_parameters": {"power_law_index": -9},
        "simulation_metadata": {"power_law_index": "E^-9"},
    },
    {
        "steering_parameters": {"power_law_index": 9},
        "simulation_metadata": {"power_law_index": "E^-9"},
    },
    {
        "steering_parameters": {"power_law_index": -0},
        "simulation_metadata": {"power_law_index": "E^0"},  # not "E^-0"
    },
    {
        "steering_parameters": {"power_law_index": 0},
        "simulation_metadata": {"power_law_index": "E^0"},  # not "E^-0"
    },
    {
        "steering_parameters": {"power_law_index": 9.5},
        "simulation_metadata": {"power_law_index": "E^-9.5"},
    },
    {
        "steering_parameters": {"CORSIKA::spectrum": 5, "spectrum": "FOOBAR"},
        "simulation_metadata": {"power_law_index": "FOOBAR"},
    },
    {
        "steering_parameters": {"power_law_index": ""},
        "simulation_metadata": {"power_law_index": ""},
    },
    # real examples
    {
        "steering_parameters": {
            # truncated from dataset #20900
            "year": 2016,
            "category": "CORSIKA-in-ice",  # 1st precedence
            "MCType": "corsika_weighted",  # 2nd precedence
            "mjd_16": 57531,
            "gcdfile": "/cvmfs/icecube.opensciencegrid.org/data/GCD/GeoCalibDetectorStatus_AVG_55697-57531_PASS2_SPE_withStdNoise.i3.gz",
            "CORSIKA::eprimarymin": 600,
            "CORSIKA::eprimarymax": 100000000,
            "CORSIKA::showers": 100000,
            "CORSIKA::spectrum": -2,
            "hadronicinteraction": "Sybill-2.3",
            "spectrum": "E^-2",
            "geometry": "IC86.2016",
            "IceSimPath": "/cvmfs/icecube.opensciencegrid.org/py2-v3.0.1/metaprojects/simulation/V06-01-01",
            "TARGET::2016": "gsiftp://gridftp.icecube.wisc.edu/data/sim/IceCube/2016/generated/$(category)/$(dataset)/$(subdirectory)",
        },
        "simulation_metadata": {
            "GCD_file": "/cvmfs/icecube.opensciencegrid.org/data/GCD/GeoCalibDetectorStatus_AVG_55697-57531_PASS2_SPE_withStdNoise.i3.gz",
            "energy_max": 100000000.0,
            "energy_min": 600.0,
            "generator": "CORSIKA-in-ice",
            "geometry": "IC86.2016",
            "hadronic_interaction": "Sybill-2.3",
            "n_events": 100000,
            "power_law_index": "E^-2",
        },
    },
    # ...
    {"steering_parameters": {}, "simulation_metadata": {}},
    {"steering_parameters": {}, "simulation_metadata": {}},
    {"steering_parameters": {}, "simulation_metadata": {}},
]
