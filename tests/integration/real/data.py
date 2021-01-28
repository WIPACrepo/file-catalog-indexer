"""Example file metadata."""

EXAMPLES = {
    "PFRaw/SPS-CV-DATA-PFRaw_TestData_RandomFiltering_Run00115379_Subrun00000001_00000000.tar.gz": {
        "_links": {
            "parent": {"href": "/api/files"},
            "self": {"href": "/api/files/d5408af6-7ae1-11ea-a6c4-3a952b566ed1"},
        },
        "checksum": {
            "sha512": "8f6494c6362627363bad618f6b442e009fb682ccd342b897fc50eaabfcb39ebc749adbc801722ad2588fa3965d2ffbf931209d3ebd1084a89cdd947d9e596d38"
        },
        "content_status": "good",
        "create_date": "2010-02-01",
        "data_type": "real",
        "file_size": 944802324,
        "locations": [
            {
                "site": "WIPAC",
                "path": "/data/exp/IceCube/2010/calibration/SouthPole/0131/SPS-CV-DATA-PFRaw_TestData_RandomFiltering_Run00115379_Subrun00000001_00000000.tar.gz",
            }
        ],
        "logical_name": "/data/exp/IceCube/2010/calibration/SouthPole/0131/SPS-CV-DATA-PFRaw_TestData_RandomFiltering_Run00115379_Subrun00000001_00000000.tar.gz",
        "meta_modify_date": "2020-04-10 04:14:55.222933",
        "processing_level": "PFRaw",
        "run": {
            "end_datetime": "2010-01-31T02:27:22",
            "event_count": 48907,
            "first_event": 8766,
            "last_event": 57672,
            "part_number": 0,
            "run_number": 115379,
            "start_datetime": None,
            "subrun_number": 1,
        },
        "software": None,
        "uuid": "d5408af6-7ae1-11ea-a6c4-3a952b566ed1",
    },
    "PFFilt/PFFilt_PhysicsTrig_PhysicsFilt_Run00087226_00025.tar.gz": {
        "_links": {
            "parent": {"href": "/api/files"},
            "self": {"href": "/api/files/9bde3ce6-7b15-11ea-93ab-3a952b566ed1"},
        },
        "checksum": {
            "sha512": "2a13cde3d9093d3d6667dc7e4e3a6d885da44a00cd2a0f42fced277af7f2fb61952a7b50cb1bfa8c78fddd01cc6ca614da08295e78043828151a242426784daf"
        },
        "content_status": "good",
        "create_date": "2006-04-01",
        "data_type": "real",
        "file_size": 1667432,
        "locations": [
            {
                "site": "WIPAC",
                "path": "/data/exp/IceCube/2010/unbiased/AURA_Processed/Oct27/data/ana/IC9/filtered/PFFilt/0401/PFFilt_PhysicsTrig_PhysicsFilt_Run00087226_00025.tar.gz",
            }
        ],
        "logical_name": "/data/exp/IceCube/2010/unbiased/AURA_Processed/Oct27/data/ana/IC9/filtered/PFFilt/0401/PFFilt_PhysicsTrig_PhysicsFilt_Run00087226_00025.tar.gz",
        "meta_modify_date": "2020-04-10 10:25:32.778472",
        "processing_level": "PFFilt",
        "run": {
            "end_datetime": "2006-04-01T15:06:08",
            "event_count": 0,
            "first_event": None,
            "last_event": None,
            "part_number": 25,
            "run_number": 87226,
            "start_datetime": "2006-04-01T15:05:06",
            "subrun_number": 0,
        },
        "software": [{"name": "PhysicsFilt", "version": "01.00.00"}],
        "uuid": "9bde3ce6-7b15-11ea-93ab-3a952b566ed1",
    },
    "PFDST/PFDST_PhysicsTrig_PhysicsFiltering_Run00116892_Subrun00000000_00000134.tar.gz": {
        "_links": {
            "parent": {"href": "/api/files"},
            "self": {"href": "/api/files/87663bc2-7b1f-11ea-9e1d-3a952b566ed1"},
        },
        "checksum": {
            "sha512": "86fec738daab38774e81a70bdb93675cb53aaf42836ecb19620c4c814f16c53fcea09997b173d0c7c8f2c79072f77ec814e9f6102bd4de6064d3f66e767e11ce"
        },
        "content_status": "good",
        "create_date": "2014-04-30T07:30:09",
        "data_type": "real",
        "file_size": 286937687,
        "locations": [
            {
                "site": "WIPAC",
                "path": "/data/exp/IceCube/2010/unbiased/PFDST/1116/PFDST_PhysicsTrig_PhysicsFiltering_Run00116892_Subrun00000000_00000134.tar.gz",
            }
        ],
        "logical_name": "/data/exp/IceCube/2010/unbiased/PFDST/1116/PFDST_PhysicsTrig_PhysicsFiltering_Run00116892_Subrun00000000_00000134.tar.gz",
        "meta_modify_date": "2020-04-10 11:36:33.404894",
        "processing_level": "PFDST",
        "run": {
            "end_datetime": "2010-11-16T19:17:34.6730371",
            "event_count": 407730,
            "first_event": 54644725,
            "last_event": 55052454,
            "part_number": 134,
            "run_number": 116892,
            "start_datetime": "2010-11-16T19:14:40.3904744109",
            "subrun_number": 0,
        },
        "software": [
            {"name": "filterscripts", "version": "V15-05-01"},
            {"name": "icerec", "version": "IC2011-L2_V12-08-00"},
        ],
        "uuid": "87663bc2-7b1f-11ea-9e1d-3a952b566ed1",
    },
    "L2/Level2_IC86.2014_data_Run00124852_Subrun00000149.i3.bz2": {
        "_links": {
            "parent": {"href": "/api/files"},
            "self": {"href": "/api/files/c93dddd4-8316-11ea-ad8b-26f2811e2864"},
        },
        "checksum": {
            "sha512": "84ef8cb97163b6f6f6843505709821b2412b513b4c76a81f9a57c94673b41d250d03e15bdebd8846c04f4f6dfa01f53c8c8567cb585cd07334dc0ed6dbdfb357"
        },
        "content_status": "good",
        "create_date": "2018-07-09",
        "data_type": "real",
        "file_size": 214733276,
        "locations": [
            {
                "site": "WIPAC",
                "path": "/data/exp/IceCube/2014/filtered/level2/0609/Run00124852_0/Level2_IC86.2014_data_Run00124852_Subrun00000149.i3.bz2",
            },
            {
                "site": "NERSC",
                "path": "/home/projects/icecube/data/exp/IceCube/2014/filtered/level2/0609/33b533dc3d5d11eba8bbce4f898b144a.zip:/data/exp/IceCube/2014/filtered/level2/0609/Run00124852_0/Level2_IC86.2014_data_Run00124852_Subrun00000149.i3.bz2",
                "archive": True,
            },
        ],
        "logical_name": "/data/exp/IceCube/2014/filtered/level2/0609/Run00124852_0/Level2_IC86.2014_data_Run00124852_Subrun00000149.i3.bz2",
        "meta_modify_date": "2020-12-13 18:27:56.220169",
        "offline_processing_metadata": {
            "L2_gcd_file": "/data/exp/IceCube/2014/filtered/level2/0609/Run00124852_0/Level2_IC86.2014_data_Run00124852_0609_0_94_GCD.i3.gz",
            "first_event": {
                "datetime": "2014-06-09T11:43:08.405840",
                "event_id": 40915942,
            },
            "gaps": [
                {
                    "start_event_id": 40915942,
                    "stop_event_id": 41190058,
                    "delta_time": 105.400041,
                    "start_date": "2014-06-09T11:43:08.405840",
                    "stop_date": "2014-06-09T11:44:53.805881",
                }
            ],
            "last_event": {
                "datetime": "2014-06-09T11:44:53.805881",
                "event_id": 41190058,
            },
            "livetime": 105.4,
            "season": "2014",
            "season_name": "IC86-4",
        },
        "processing_level": "L2",
        "run": {
            "end_datetime": None,
            "event_count": 194351,
            "first_event": 40915942,
            "last_event": 41190058,
            "part_number": 149,
            "run_number": 124852,
            "start_datetime": None,
            "subrun_number": 0,
        },
        "software": None,
        "uuid": "c93dddd4-8316-11ea-ad8b-26f2811e2864",
    },
}
