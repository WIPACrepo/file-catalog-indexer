"""Utilities for metadata collection."""


import os
from enum import Enum
from typing import Dict, Optional


class FileInfo:  # pylint: disable=R0903
    """Wrapper around common file information.

    Similar to os.DirEntry.
    """

    def __init__(self, filepath: str):
        self.path = filepath
        self.name = os.path.basename(self.path)
        self.stat = lambda: os.stat(self.path)


class IceCubeSeasonException(Exception):
    """Raised when there's a problem detecting the season/year."""


class IceCubeSeason:
    """Wrapper static class encapsulating season-name - season-year mapping logic."""

    SEASONS: Dict[int, str] = {
        2005: "ICstring9",
        2006: "IC9",
        2007: "IC22",
        2008: "IC40",
        2009: "IC59",
        2010: "IC79",
        2011: "IC86-1",
        2012: "IC86-2",
        2013: "IC86-3",
        2014: "IC86-4",
        2015: "IC86-5",
        2016: "IC86-6",
        2017: "IC86-7",
        2018: "IC86-8",
        2019: "IC86-9",
        2020: "IC86-10",
        2021: "IC86-11",
    }

    @staticmethod
    def name_to_year(name: Optional[str]) -> Optional[int]:
        """Return the year of the season start for the season's `name`."""
        if not name:
            return None
        for season_year, season_name in IceCubeSeason.SEASONS.items():
            if season_name == name:
                return int(season_year)
        raise IceCubeSeasonException(f"No season-year found for season:`{name}`.")

    @staticmethod
    def year_to_name(season_year: Optional[int]) -> Optional[str]:
        """Return the season's name for the year of the season start."""
        if not season_year:
            return None
        try:
            return IceCubeSeason.SEASONS[season_year]
        except KeyError:
            raise IceCubeSeasonException(f"No season found for year:`{season_year}`.")


class ProcessingLevel(Enum):
    """Enum for processing-level constants."""

    # pylint:disable=invalid-name

    PFRaw = "PFRaw"  # exp
    PFFilt = "PFFilt"  # exp
    PFDST = "PFDST"  # exp
    Generated = "Generated"  # sim
    Propagated = "Propagated"  # sim
    Triggered = "Triggered"  # sim
    L1 = "L1"  # sim
    L2 = "L2"  # exp & sim
    L3 = "L3"  # sim
    L4 = "L4"  # sim
    L5 = "L5"  # sim
