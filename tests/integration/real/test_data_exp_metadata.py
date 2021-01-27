"""Test pipeline starting at MetadataManager.

1. manager = MetadataManager(...)
2. metadata_file = manager.new_file(filepath)
3. metadata = metadata_file.generate()
"""

EXAMPLES = {
    "PFRaw": {
        "logical_name": "/data/exp/IceCube/2007new/filtered/GRB/0911/"
        "PFRaw_PhysicsTrig_PhysicsFiltering_Run00109298_Subrun00000000_00000064.tar.gz",
        "uuid": "4a59b52c-7a46-11ea-911f-3a952b566ed1",
    },
    "PFFilt": {
        "logical_name": "/data/exp/IceCube/2006/filtered/PFFilt/0417/"
        "PFFilt_PhysicsTrig_PhysicsFilt_Run00087385_00008.tar.gz",
        "uuid": "8c85ec48-7a31-11ea-9053-3a952b566ed1",
    },
    "PFDST": {
        "logical_name": "/data/exp/IceCube/2010/unbiased/PFDST/1116/"
        "PFDST_PhysicsTrig_PhysicsFiltering_Run00116892_Subrun00000000_00000134.tar.gz",
        "uuid": "87663bc2-7b1f-11ea-9e1d-3a952b566ed1",
    },
    "L2": {
        "logical_name": "/data/exp/DM-Ice/2012/filtered/pole/icecube_coincidence/06/det1/"
        "Level2_IC86.2012_data_Run00120243_Subrun00000311.i3.bz2",
        "uuid": "387b924a-79f1-11ea-a3bc-3a952b566ed1",
    },
}
