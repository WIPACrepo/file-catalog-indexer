"""Example /data/sim i3 filepaths."""


from typing import Dict, Optional, TypedDict

from indexer.utils import utils


class _FilepathValues(TypedDict, total=False):
    proc_level: Optional[utils.ProcessingLevel]
    dataset: Optional[int]
    job: Optional[int]
    fileinfo: utils.FileInfo


EXAMPLES: Dict[str, _FilepathValues] = {
    # NOTE: first, the top 50 most common patterns...
    "/data/sim/IceCube/2011/filtered/level2/CORSIKA-in-ice/10285/00000-00999/"
    "Level2_IC86.2011_corsika.010285.000000.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 10285,
        "job": 0,
    },
    "/data/sim/IceCube/2010/generated/CORSIKA-in-ice_000674/00000-00999/"
    "corsika.000674.000000.i3.gz": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 674,
        "job": 0,
    },
    "/data/sim/IceCube/2011/generated/CORSIKA-ice-top/12333/topsimulator/0000000-0000999/"
    "TopSimulator_IC86_corsika_icetop.010410.000001.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 10410,
        "job": 1,
    },
    "/data/sim/IceCube/2016/generated/CORSIKA-in-ice/21269/IC86_2016_spe_templates_DOM_oversize1/level2.orig/eff090/0000000-0000999/"
    "Level2_eff090_IC86-2016_corsika_21269_00000.i3.zst": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 21269,
        "job": 0,
    },
    "/data/sim/IceCube/2011/filtered/level2/CORSIKA-ice-top/12333/level2/0000000-0000999/"
    "Level2_IC86_corsika_icetop.010410.000001.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 10410,
        "job": 1,
    },
    "/data/sim/IceCube/2016/generated/CORSIKA-in-ice/20900/0000000-0000999/"
    "hits.020900.000000.i3.zst": {
        "proc_level": utils.ProcessingLevel.Propagated,
        "dataset": 20900,
        "job": 0,
    },
    "/data/sim/IceCube/2016/generated/CORSIKA-in-ice/21269/IC86_2016_spe_templates_DOM_oversize1/detectorsim.orig/eff090/0000000-0000999/"
    "detector_eff090_IC86-2016_corsika_21269_00000.i3.zst": {
        "proc_level": utils.ProcessingLevel.Triggered,
        "dataset": 21269,
        "job": 0,
    },
    "/data/sim/IceCube/2010/filtered/level3-cscd/6451/000000-000999/"
    "Level3_IC79_corsika.006271.000000.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L3,
        "dataset": 6271,
        "job": 0,
    },
    "/data/sim/IceCube/2011/generated/CORSIKA-in-ice/10285/00000-00999/"
    "IC86.2011_corsika.010285.000000.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 10285,
        "job": 0,
    },
    "/data/sim/IceCube/2009/filtered/DST/4046/mmcTrackInfo/"
    "Level2_corsika_IC59.004046.000001.i3.gz": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 4046,
        "job": 1,
    },
    "/data/sim/IceCube/2011/generated/CORSIKA-ice-top/12333/detector/0000000-0000999/"
    "Detector_IC86_corsika_icetop.010410.000001.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Triggered,
        "dataset": 10410,
        "job": 1,
    },
    "/data/sim/IceCube/2011/generated/CORSIKA-ice-top/12333/topsimulator/0000000-0000999/"
    "Propagated_IC86_corsika_icetop.010410.000001.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Propagated,
        "dataset": 10410,
        "job": 1,
    },
    "/data/sim/IceCube/2019/filtered/finallevel/CORSIKA-in-ice/northern_tracks/21117/"
    "FinalLevel_IC86.2016_corsika.020881.000000.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 20881,
        "job": 0,
    },
    "/data/sim/IceCube/2016/generated/GENIE/21262/0000000-0000999/"
    "IC86.2016_NuMu.021262.000000.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 21262,
        "job": 0,
    },
    "/data/sim/IceCube/2011/filtered/level2/GENIE/11998/00000-00999/"
    "Level2_IC86.2011_genie_numu.011998.000001.00.0.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 11998,
        "job": 1,
    },
    "/data/sim/IceCube/2011/filtered/level2/GENIE/11999/00000-00999/"
    "Level2_IC86.2011_genie_nue.011999.000001.00.0.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 11999,
        "job": 1,
    },
    "/data/sim/PINGU/Phase1/pDOM/v47/processed/Step2/nutau/001003/"
    "Step2_genie_NuTau_pingu_V47.001003.000001.00.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 1003,
        "job": 1,
    },
    "/data/sim/IceCube/2012/conflicts/filtered/level2/neutrino-generator/11374/01000-01999/clsim-ellipse-abs.01.0.89_eff/"
    "Level2_IC86.2012_nugen_numu.011374.001300.clsim-ellipse-abs.01.0.89_eff.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 11374,
        "job": 1300,
    },
    "/data/sim/IceCube/2010/filtered/level2a/CORSIKA-in-ice/7238/02000-02999/"
    "Level2a_IC79_corsika_icetop.007238.002617.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 7238,
        "job": 2617,
    },
    "/data/sim/IceCube/2016/filtered/level2/GENIE/21262/0000000-0000999/"
    "Level2_IC86.2016_NuMu.021262.000000.i3.zst": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 21262,
        "job": 0,
    },
    "/data/sim/IceCube/2016/filtered/level2/CORSIKA-in-ice/20904/0000000-0000999/dst/"
    "IC86.2016_corsika.020904.000000.dst.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 20904,
        "job": 0,
    },
    "/data/sim/IceCube/2019/filtered/level3/CORSIKA-in-ice/21120/0000000-0000999/"
    "Level3_IC86.2016_corsika.020789.000001.i3.zst": {
        "proc_level": utils.ProcessingLevel.L3,
        "dataset": 20789,
        "job": 1,
    },
    "/data/sim/IceCube/2010/filtered/level2a/CORSIKA-in-ice/5959/00000-00999/"
    "Level2a_IC79_corsika.005959.000020.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 5959,
        "job": 20,
    },
    "/data/sim/DeepCore/2013/filtered/level2/12000/"
    "genie_ic.12000.000000.i3.gz": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 12000,
        "job": 0,
    },
    "/data/sim/IceCube/2013/generated/CORSIKA-in-ice/photo-electrons/briedel/10700/atmod_0/"
    "unweighted_0.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 0,
        "job": None,
    },
    "/data/sim/IceCube/2010/filtered/level2/CORSIKA-in-ice/10664/00000-00999/"
    "Level2_IC79_corsika.010664.000000.i3.gz": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 10664,
        "job": 0,
    },
    "/data/sim/IceCube/2011/filtered/level2/GENIE/11539/00000-00999/"
    "Level2_genie_numu_IC86.011539.000000.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 11539,
        "job": 0,
    },
    "/data/sim/IceCube/2011/filtered/level2/CORSIKA-in-ice/10281/00000-00999/"
    "Level2_IC86.2011_corsika.010281.000000.00.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 10281,
        "job": 0,
    },
    "/data/sim/IceCube/2016/generated/GENIE/21379/bulk-ice/mcpes/0000000-0000999/err_s+.05_a+.05/"
    "genie_NuMu.021379.000000.mcpes.err_s+.05_a+.05.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 21379,
        "job": 0,
    },
    "/data/sim/IceCube/2009/filtered/level2/Template/6303/00000-00999/"
    "Level2_nugen_numu_IC59.006303.000000.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 6303,
        "job": 0,
    },
    "/data/sim/IceCube/2012/generated/CORSIKA-in-ice/12359/0000000-0000999/"
    "corsika_12359_0.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 12359,
        "job": 0,
    },
    "/data/sim/IceCube/2016/generated/MuonGun/20883/0000000-0000999/"
    "IC86.2016_MuonGun.020883.000000.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 20883,
        "job": 0,
    },
    "/data/sim/IceCube/2016/filtered/level2/MuonGun/20883/0000000-0000999/"
    "Level2_IC86.2016_MuonGun.020883.000000.i3.zst": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 20883,
        "job": 0,
    },
    "/data/sim/IceCube/2016/generated/GENIE/21379/detector/bulk-ice/err_s+.05_a+.05/0000000-0000999/"
    "IC86.2016.genie_NuMu.021379.000000.err_s+.05_a+.05.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 21379,
        "job": 0,
    },
    "/data/sim/IceCube/2016/filtered/level2/GENIE/21379/bulk-ice/err_s+.05_a+.05/0000000-0000999/"
    "Level2_IC86.2016.genie_NuMu.021379.000000.err_s+.05_a+.05.i3.zst": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 21379,
        "job": 0,
    },
    "/data/sim/IceCube/2010/filtered/level2/CORSIKA-in-ice/10664/00000-00999/"
    "Level2_IC79_corsika.010664.000000_SLOP.i3.gz": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 10664,
        "job": 0,
    },
    "/data/sim/IceCube/2019/generated/CORSIKA-in-ice/21248/0000000-0000999/background/test/"
    "corsika_bg_21248_00000.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 21248,
        "job": 0,
    },
    "/data/sim/IceTop/2008/filtered/level2b/CORSIKA-ice-top/2558/"
    "level2b.corsika_it_40_ic40.002558.000000.DAT000001.i3.gz": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 2558,
        "job": 0,
    },
    "/data/sim/PINGU/Phase1/pDOM/v47/processed/Step2/numu/001003/"
    "Step2_genie_NuMu_pingu_V47.001003.000001.00.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 1003,
        "job": 1,
    },
    "/data/sim/IceCube/2011/generated/GENIE/10746/00000-00999/"
    "genie_numu_IC86.010746.000000.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 10746,
        "job": 0,
    },
    "/data/sim/IceCube/2016/generated/GENIE/21405/holeice/mcpes/0000000-0000999/as.flasher_p1_0.25_p2_+1/"
    "genie_NuE.021405.000001.mcpes.as.flasher_p1_0.25_p2_+1.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 21405,
        "job": 1,
    },
    "/data/sim/IceCube/2016/generated/GENIE/21405/detector/holeice/as.flasher_p1_0.25_p2_+1/0000000-0000999/"
    "IC86.2016.genie_NuE.021405.000000.as.flasher_p1_0.25_p2_+1.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 21405,
        "job": 0,
    },
    "/data/sim/IceCube/2016/filtered/level2/GENIE/21405/holeice/as.flasher_p1_0.25_p2_+1/0000000-0000999/"
    "Level2_IC86.2016.genie_NuE.021405.000000.as.flasher_p1_0.25_p2_+1.i3.zst": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 21405,
        "job": 0,
    },
    "/data/sim/IceCube/2016/generated/GENIE/21388/holeice/mcpes/0000000-0000999/as.flasher_p1_0.25_p2_+1/"
    "genie_NuTau.021388.000000.mcpes.as.flasher_p1_0.25_p2_+1.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 21388,
        "job": 0,
    },
    "/data/sim/IceCube/2016/generated/GENIE/21388/detector/holeice/as.flasher_p1_0.25_p2_+1/0000000-0000999/"
    "IC86.2016.genie_NuTau.021388.000000.as.flasher_p1_0.25_p2_+1.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 21388,
        "job": 0,
    },
    "/data/sim/IceCube/2016/filtered/level2/GENIE/21388/holeice/as.flasher_p1_0.25_p2_+1/0000000-0000999/"
    "Level2_IC86.2016.genie_NuTau.021388.000000.as.flasher_p1_0.25_p2_+1.i3.zst": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 21388,
        "job": 0,
    },
    "/data/sim/IceCube/2009/filtered/IC59diffuse/4703/"
    "Level4_corsika_IC59.004703.000099.i3.gz": {
        "proc_level": utils.ProcessingLevel.L4,
        "dataset": 4703,
        "job": 99,
    },
    "/data/sim/IceCube/2012/conflicts/filtered/level2/neutrino-generator/11491/00000-00999/clsim-base-4.0.3.0.89_eff/"
    "Level2_IC86.2012_nugen_nutau.011491.000584.clsim-base-4.0.3.0.89_eff.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 11491,
        "job": 584,
    },
    "/data/sim/IceCube/2012/filtered/level2/neutrino-generator/11813/00000-00999/clsim-base-4.0.3.0.89_eff/"
    "Level2_IC86.2012_nugen_nue.011813.000000.clsim-base-4.0.3.0.89_eff.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 11813,
        "job": 0,
    },
    "/data/sim/IceCube/2011/filtered/level2/GENIE/12000/00000-00999/"
    "Level2_IC86.2011_genie_nutau.012000.000003.01.0.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 12000,
        "job": 3,
    },
    # NOTE: the rest of these are cherry picked for variety...
    "/data/sim/PINGU/2013/processed/multiNestProcessedwPID/199/"
    "level1_genie_pinguv15_numu_postEBugFix_rangeCut.000199.000000_weighted_deltaCP_0_MultiNest_PID.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L1,
        "dataset": 199,
        "job": 0,
    },
    "/data/sim/IceCube/2011/generated/GENIE-in-ice/PINGU/numu/60008/"
    "genie_ic.60008.000000.L3.i3.gz": {
        "proc_level": utils.ProcessingLevel.L3,
        "dataset": 60008,
        "job": 0,
    },
    "/data/sim/IceCube/2011/filtered/level2/GENIE-in-ice/level2/nue/"
    "genie_ic.70070.0.L2a.i3": {
        "proc_level": utils.ProcessingLevel.L2,
        "dataset": 70070,
        "job": 0,
    },
    "/data/sim/IceCube/2013/generated/CORSIKA-in-ice/photo-electrons/briedel/tania/1450/"
    "Level5b_IC86.2012_genie_numu.001450.000000.i3.bz2": {
        "proc_level": utils.ProcessingLevel.L5,
        "dataset": 1450,
        "job": 0,
    },
    "/data/sim/IceCube/2011/generated/EHE/CORSIKA/V02-01-02/corsika/qgsii/iron/0001_0499/"
    "hit000008_1_0.i3.gz": {
        "proc_level": utils.ProcessingLevel.Propagated,
        "dataset": 8,
        "job": 1,
    },
    "/data/sim/IceCube/2012/conflicts/generated/neutrino-generator/11374/03000-03999/clsim-base-4.0.3.0.89_eff/"
    "IC86.2012_nugen_numu.011374.003467.clsim-base-4.0.3.0.89_eff.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 11374,
        "job": 3467,
    },
    "/data/sim/IceCube/2016/generated/neutrino-generator/21468/0000000-0000999/"
    "NuE_NuGenCCNC.021468.000000.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 21468,
        "job": 0,
    },
    "/data/sim/IceCube/2020/generated/SN/MCSNMakeCLSim5MeV/"
    "MCSNInjector00001.i3.gz": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 1,
        "job": None,
    },
    "/data/sim/IceCube/2013/generated/CORSIKA-in-ice/photo-electrons/briedel/snsample/mcgill/"
    "SimpleInjector_RunSimpleGenerator.pbs_50629255.gm-1r16-n04.guillimin.clumeq.ca.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 4,
        "job": None,
    },
    "/data/sim/PINGU/2014/triggered/MuonGun/IC86/trigger/000029/generator/"
    "muongun_IC2012.000029.000000.generated.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 29,
        "job": 0,
    },
    "/data/sim/IceCube/2016/generated/neutrino-generator/21430/0000000-0000999/"
    "NuMu_NuGenCCNC.021430.000000.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 21430,
        "job": 0,
    },
    "/data/sim/IceCube/2016/generated/GENIE/21352/0000000-0000999/"
    "IC86.2016_NuE.021352.000000.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 21352,
        "job": 0,
    },
    "/data/sim/IceCube/2016/generated/GENIE/21351/0000000-0000999/"
    "IC86.2016_NuTau.021351.000000.i3.zst": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 21351,
        "job": 0,
    },
    "/data/sim/IceCube/2010/generated/JULIeT/IC-59/nutau/slope-1/"
    "juliet_nutau_slope1_ic59.000000.000000.i3.gz": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 0,
        "job": 0,
    },
    "/data/sim/PINGU/2014/triggered/MuonGun/IC86/trigger/000025/clsim/"
    "muongun_IC2012.000025.000000.clsim.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 25,
        "job": 0,
    },
    "/data/sim/IceCube/2011/filtered/level2/Monopoles/KYG_055/L0_ppc_mie/"
    "Monopole_KYG1_055_00000.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 55,
        "job": 0,
    },
    "/data/sim/IceCube/2011/filtered/level2/Monopoles/KYG_040_0990_i5/L0_AbsScatt_m10/"
    "Monopoles_0000.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 0,
        "job": None,
    },
    "/data/sim/IceCube/2016/filtered/level2/Monopoles/MonoSim_00000001/"
    "MonoSim_00000001_00000001_00001000_00001000_00000000_01_08_0_lv_2.i3.bz2": {
        "proc_level": utils.ProcessingLevel.Generated,
        "dataset": 0,
        "job": 1,
    },
    # "ZZZ": {"proc_level": 0, "dataset": 0, "job": 0},
    # "ZZZ": {"proc_level": 0, "dataset": 0, "job": 0},
    # "ZZZ": {"proc_level": 0, "dataset": 0, "job": 0},
}

# NOTE: add FileInfos
for fpath in list(EXAMPLES.keys()):
    EXAMPLES[fpath]["fileinfo"] = utils.FileInfo(fpath)
