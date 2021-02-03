"""Regex collections for filename patterns."""


from typing import List

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict


class _Patterns(TypedDict):
    """Filename patterns for a processing level."""

    base_pattern: str
    patterns: List[str]


# --------------------------------------------------------------------------------------


L2: _Patterns = {
    # Ex: Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188.i3.zst
    # check if last char of filename (w/o extension) is an int
    "base_pattern": r".*Level2.*Run(\d+)_.*\d\.i3",
    #
    "patterns": [
        # Ex: Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst
        # Ex: Level2pass2_IC79.2010_data_Run00115975_Subrun00000000_00000055.i3.zst
        # Ex: Level2_IC86.2018RHEL_6_V05-02-00b_py2-v311_data_Run00132765_Subrun00000000_00000000.i3.zst
        r".*\.(?P<year>20\d{2}).*_data_Run(?P<run>\d+)_Subrun(?P<subrun>\d+)_(?P<part>\d+)\.",
        #
        # Ex: Level2_PhysicsTrig_PhysicsFiltering_Run00120374_Subrun00000000_00000001.i3
        # Ex: Level2pass3_PhysicsFiltering_Run00127353_Subrun00000000_00000000.i3.gz
        # Ex: Level2_PhysicsTrig_PhysicsFiltering_Run00120374_Subrun00000000_00000001_new2.i3
        r".*_PhysicsFiltering_Run(?P<run>\d+)_Subrun(?P<subrun>\d+)_(?P<part>\d+)(_new\d+)?\.",
        #
        # Ex: Level2_IC86.2016_data_Run00129004_Subrun00000316.i3.bz2
        # Ex: Level2_IC86.2012_Test_data_Run00120028_Subrun00000081.i3.bz2
        # Ex: Level2_IC86.2015_24HrTestRuns_data_Run00126291_Subrun00000203.i3.bz2
        r".*\.(?P<year>20\d{2})_.*data_Run(?P<run>\d+)_Subrun(?P<part>\d+)\.",
        #
        # Ex: Level2_IC86.2011_data_Run00119221_Part00000126.i3.bz2
        r".*\.(?P<year>20\d{2})_data_Run(?P<run>\d+)_Part(?P<part>\d+)\.",
        #
        # Ex: Level2a_IC59_data_Run00115968_Part00000290.i3.gz
        # Ex: MoonEvents_Level2_IC79_data_Run00116082_NewPart00000613.i3.gz
        r".*_IC(?P<ic_strings>\d+)_data_Run(?P<run>\d+)_(New)?Part(?P<part>\d+)\.",
        #
        # Ex: Level2_All_Run00111562_Part00000046.i3.gz
        # Ex: MoonEvents_Level2_All_Run00111887_part2.i3.gz
        r".*_All_Run(?P<run>\d+)_[Pp]art(?P<part>\d+)\.",
    ],
}

# --------------------------------------------------------------------------------------


PFFilt: _Patterns = {
    # Ex. PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2
    "base_pattern": r".*PFFilt.*Run(\d+)_.*\d\.tar\.(gz|bz2|zst)",
    #
    "patterns": [
        # Ex: PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2
        # Ex: PFFilt_PhysicsTrig_PhysicsFiltering_Run00121503_Subrun00000000_00000314.tar.bz2
        # Ex: orig.PFFilt_PhysicsFiltering_Run00127080_Subrun00000000_00000244.tar.bz2.orig
        r".*PFFilt_.*_Run(?P<run>\d+)_Subrun(?P<subrun>\d+)_(?P<part>\d+)\.",
        #
        # Ex: PFFilt_PhysicsTrig_PhysicsFilt_Run00089959_00180.tar.gz
        # Ex: PFFilt_PhysicsTrig_RandomFilt_Run86885_006.tar.gz
        r"PFFilt_.*_Run(?P<run>\d+)_(?P<part>\d+)\.",
    ],
}

# --------------------------------------------------------------------------------------


PFDST: _Patterns = {
    # Ex. ukey_fa818e64-f6d2-4cc1-9b34-e50bfd036bf3_PFDST_PhysicsFiltering_Run00131437_Subrun00000000_00000066.tar.gz
    "base_pattern": r".*PFDST.*Run(\d+)_.*\d\.tar\.(gz|bz2|zst)",
    #
    "patterns": [
        # Ex. ukey_fa818e64-f6d2-4cc1-9b34-e50bfd036bf3_PFDST_PhysicsFiltering_Run00131437_Subrun00000000_00000066.tar.gz
        # Ex: ukey_42c89a63-e3f7-4c3e-94ae-840eff8bd4fd_PFDST_RandomFiltering_Run00131155_Subrun00000051_00000000.tar.gz
        # Ex: PFDST_PhysicsFiltering_Run00125790_Subrun00000000_00000064.tar.gz
        # Ex: PFDST_UW_PhysicsFiltering_Run00125832_Subrun00000000_00000000.tar.gz
        # Ex: PFDST_RandomFiltering_Run00123917_Subrun00000000_00000000.tar.gz
        # Ex: PFDST_PhysicsTrig_PhysicsFiltering_Run00121663_Subrun00000000_00000091.tar.gz
        # Ex: PFDST_TestData_PhysicsFiltering_Run00122158_Subrun00000000_00000014.tar.gz
        # Ex: PFDST_TestData_RandomFiltering_Run00119375_Subrun00000136_00000000.tar.gz
        # Ex: PFDST_TestData_Unfiltered_Run00119982_Subrun00000000_000009.tar.gz
        r".*_Run(?P<run>\d+)_Subrun(?P<subrun>\d+)_(?P<part>\d+)\."
    ],
}

# --------------------------------------------------------------------------------------


PFRaw: _Patterns = {
    # Ex. key_31445930_PFRaw_PhysicsFiltering_Run00128000_Subrun00000000_00000156.tar.gz
    "base_pattern": r".*PFRaw.*Run\d+.*\d\.tar\.(gz|bz2|zst)",
    #
    "patterns": [
        # Ex: key_31445930_PFRaw_PhysicsFiltering_Run00128000_Subrun00000000_00000156.tar.gz
        # Ex: ukey_b98a353f-72e8-4d2e-afd7-c41fa5c8d326_PFRaw_PhysicsFiltering_Run00131322_Subrun00000000_00000018.tar.gz
        # Ex: ukey_05815dd9-2411-468c-9bd5-e99b8f759efd_PFRaw_RandomFiltering_Run00130470_Subrun00000060_00000000.tar.gz
        # Ex: PFRaw_PhysicsTrig_PhysicsFiltering_Run00114085_Subrun00000000_00000208.tar.gz
        # Ex: PFRaw_TestData_PhysicsFiltering_Run00114672_Subrun00000000_00000011.tar.gz
        # Ex: PFRaw_TestData_RandomFiltering_Run00113816_Subrun00000033_00000000.tar.gz
        r".*_Run(?P<run>\d+)_Subrun(?P<subrun>\d+)_(?P<part>\d+)\.",
        #
        # Ex: EvtMonPFRaw_PhysicsTrig_RandomFiltering_Run00106489_Subrun00000000.tar.gz
        r".*_Run(?P<run>\d+)_Subrun(?P<part>\d+)\.",
        #
        # Ex: DebugData_PFRaw_Run110394_1.tar.gz
        r".*_Run(?P<run>\d+)_(?P<part>\d+)\.",
        #
        # Ex: DebugData-PFRaw_RF_Run00129335_SR00_00.tar.gz.tar.gz
        r".*_Run(?P<run>\d+)_SR(?P<part>\d+)_\d+\.",
        #
        # Ex: DebugData-missing_PFRaw_data_Run129969_21_to_24.tar.gz
        r".*_Run(?P<run>\d+)_\d+_to_\d+\.",
        #
        # Ex: DebugData_PFRaw_TestData_PhysicsFiltering_Run00111448.tar.gz
        # Ex: DebugData-PFRaw_TestData_Run00118957.tar.gz
        # Ex: DebugData-PFRaw_flasher_Run130047.tar.gz
        r".*DebugData.*_Run(?P<run>\d+)\.",
        #
        # Ex: EvtMonPFRaw_PhysicsTrig_RandomFilt_Run86510.tar.gz
        r".*EvtMon.*_Run(?P<run>\d+)\.",
    ],
}

# --------------------------------------------------------------------------------------
