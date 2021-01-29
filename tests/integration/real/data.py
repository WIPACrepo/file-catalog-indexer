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
    "L2/Level2_IC86.2018_data_Run00131410_Subrun00000000_00000172.i3.zst": {
        "_links": {
            "parent": {"href": "/api/files"},
            "self": {"href": "/api/files/c69f23f8-84ac-11ea-ac32-26f2811e2864"},
        },
        "checksum": {
            "sha512": "1fb4f61a62c05f4a955abb8f83fd72b463e2727086abcbda9213a496e51ef6de29952f7d9c551b5807d75e7cbbafbaae3ba4491c91a6bb2e872b562a7e26b591"
        },
        "content_status": "good",
        "create_date": "2018-08-20",
        "data_type": "real",
        "file_size": 159646051,
        "locations": [
            {
                "site": "WIPAC",
                "path": "/data/exp/IceCube/2018/filtered/level2/0820/Run00131410_74/Level2_IC86.2018_data_Run00131410_Subrun00000000_00000172.i3.zst",
            }
        ],
        "logical_name": "/data/exp/IceCube/2018/filtered/level2/0820/Run00131410_74/Level2_IC86.2018_data_Run00131410_Subrun00000000_00000172.i3.zst",
        "meta_modify_date": "2020-05-06 22:53:44.429119",
        "offline_processing_metadata": {
            "L2_gcd_file": "/data/exp/IceCube/2018/filtered/level2/0820/Run00131410_74/Level2_IC86.2018_data_Run00131410_0820_74_409_GCD.i3.zst",
            "first_event": {
                "datetime": "2018-08-20T19:05:11.299192",
                "event_id": 52027135,
            },
            "gaps": [
                {
                    "start_event_id": 52027135,
                    "stop_event_id": 52331521,
                    "delta_time": 118.960197,
                    "start_date": "2018-08-20T19:05:11.299192",
                    "stop_date": "2018-08-20T19:07:10.259389",
                }
            ],
            "last_event": {
                "datetime": "2018-08-20T19:07:10.259389",
                "event_id": 52331521,
            },
            "livetime": 118.96,
            "season": "2018",
            "season_name": "IC86-8",
        },
        "processing_level": "L2",
        "run": {
            "end_datetime": "2018-08-20T20:44:54",
            "event_count": 177992,
            "first_event": 52027135,
            "last_event": 52331521,
            "part_number": 172,
            "run_number": 131410,
            "start_datetime": "2018-08-20T13:25:29",
            "subrun_number": 0,
        },
        "software": [
            {"name": "icerec", "version": "V05-02-00", "date": "2018-09-12T18:25:48"},
            {
                "name": "http://code.icecube.wisc.edu/svn/sandbox/jan/OfflineSubmitScripts/seasons/2017/PostProcessing_L2.py",
                "version": "Revision 157547",
                "date": "2018-09-15T14:13:17",
            },
        ],
        "uuid": "c69f23f8-84ac-11ea-ac32-26f2811e2864",
    },
}
