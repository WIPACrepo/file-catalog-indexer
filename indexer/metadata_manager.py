"""Class for managing metadata collection / interfacing with indexer.py."""


import logging
import os
import re
import tarfile
import typing
import xml
from typing import Any, Dict

import xmltodict  # type: ignore[import]
import yaml

from .metadata import basic, i3, real
from .utils import utils


class MetadataManager:  # pylint: disable=R0903
    """Commander class for handling metadata for different file types."""

    def __init__(self, site: str, basic_only: bool = False):
        self.dir_path = ""
        self.site = site
        self.basic_only = basic_only
        self.l2_dir_metadata: Dict[str, Dict[str, Any]] = {}

    def _prep_l2_dir_metadata(self) -> None:
        """Get metadata files for later processing with individual i3 files."""
        self.l2_dir_metadata = {}
        dir_meta_xml = None
        gaps_files = {}  # gaps_files[<filename w/o extension>]
        gcd_files = {}  # gcd_files[<run id w/o leading zeros>]

        for dir_entry in os.scandir(self.dir_path):
            if not dir_entry.is_file():
                continue

            # Meta XML (one per directory)
            # Ex. level2_meta.xml, level2pass2_meta.xml
            if re.match(r"level2.*meta\.xml$", dir_entry.name):
                if dir_meta_xml is not None:
                    raise Exception(
                        f"Multiple level2*meta.xml files found in {self.dir_path}."
                    )
                try:
                    with open(dir_entry.path, "r") as xml_file:
                        dir_meta_xml = xmltodict.parse(xml_file.read())
                        dir_meta_xml = typing.cast(Dict[str, Any], dir_meta_xml)
                    logging.debug(f"Grabbed level2*meta.xml file, {dir_entry.name}.")
                except xml.parsers.expat.ExpatError:
                    pass

            # Gaps Files (one per i3 file)
            # Ex. Run00130484_GapsTxt.tar
            elif "_GapsTxt.tar" in dir_entry.name:
                try:
                    with tarfile.open(dir_entry.path) as tar:
                        for tar_obj in tar:
                            # pylint: disable=C0325
                            if not (iobytes := tar.extractfile(tar_obj)):
                                continue
                            file_dict = yaml.safe_load(iobytes)
                            # Ex. Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188_gaps.txt
                            no_extension = tar_obj.name.split("_gaps.txt")[0]
                            gaps_files[no_extension] = file_dict
                            logging.debug(
                                f"Grabbed gaps file for '{no_extension}', {dir_entry.name}."
                            )
                except tarfile.ReadError:
                    pass

            # GCD Files (one per run)
            # Ex. Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst
            elif "GCD" in dir_entry.name:
                run = i3.I3FileMetadata.parse_run_number(dir_entry.name)
                gcd_files[str(run)] = dir_entry.path
                logging.debug(f"Grabbed GCD file for run {run}, {dir_entry.name}.")

        self.l2_dir_metadata["dir_meta_xml"] = dir_meta_xml if dir_meta_xml else {}
        self.l2_dir_metadata["gaps_files"] = gaps_files
        self.l2_dir_metadata["gcd_files"] = gcd_files

    def new_file(self, filepath: str) -> basic.BasicFileMetadata:
        """Return different metadata-file objects.

        Factory method.
        """
        file = utils.FileInfo(filepath)
        if not self.basic_only:
            # L2
            if real.l2.L2FileMetadata.is_valid_filename(file.name):
                # get directory's metadata
                file_dir_path = os.path.dirname(os.path.abspath(file.path))
                if (not self.l2_dir_metadata) or (file_dir_path != self.dir_path):
                    self.dir_path = file_dir_path
                    self._prep_l2_dir_metadata()
                try:
                    no_extension = file.name.split(".i3")[0]
                    gaps = self.l2_dir_metadata["gaps_files"][no_extension]
                except KeyError:
                    gaps = {}
                try:
                    run = i3.I3FileMetadata.parse_run_number(file.name)
                    gcd = self.l2_dir_metadata["gcd_files"][str(run)]
                except KeyError:
                    gcd = ""
                logging.debug(f"Gathering L2 metadata for {file.name}...")
                return real.l2.L2FileMetadata(
                    file, self.site, self.l2_dir_metadata["dir_meta_xml"], gaps, gcd
                )
            # PFFilt
            if real.pffilt.PFFiltFileMetadata.is_valid_filename(file.name):
                logging.debug(f"Gathering PFFilt metadata for {file.name}...")
                return real.pffilt.PFFiltFileMetadata(file, self.site)
            # PFDST
            if real.pfdst.PFDSTFileMetadata.is_valid_filename(file.name):
                logging.debug(f"Gathering PFDST metadata for {file.name}...")
                return real.pfdst.PFDSTFileMetadata(file, self.site)
            # PFRaw
            if real.pfraw.PFRawFileMetadata.is_valid_filename(file.name):
                logging.debug(f"Gathering PFRaw metadata for {file.name}...")
                return real.pfraw.PFRawFileMetadata(file, self.site)
            # if no match, fall-through to real.BasicFileMetadata...
        # Other/ Basic
        logging.debug(f"Gathering basic metadata for {file.name}...")
        return basic.BasicFileMetadata(file, self.site)
