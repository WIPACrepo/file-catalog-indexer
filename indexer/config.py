# config.py

from typing import List, Optional, TypedDict


class IndexerConfiguration(TypedDict):
    # only post basic metadata
    basic_only: bool
    # list of denylisted filepaths; Ex: /foo/bar/ will skip /foo/bar/*
    denylist: Optional[List[str]]
    # a file containing denylisted filepaths on each line (this is a useful
    # alternative to `--denylist` when there's many denylisted paths);
    # Ex: /foo/bar/ will skip /foo/bar/*
    denylist_file: str
    # do everything except POSTing/PATCHing to the File Catalog
    dryrun: bool
    # IceProd1 SQL password
    iceprodv1_db_pass: str
    # number of processes for multi-processing (ignored if `non_recursive=True`)
    n_processes: int
    # do not recursively index / do not descend into sub-directories
    non_recursive: bool
    # replace/overwrite any existing File-Catalog entries (aka PATCH)
    patch: bool
    # path(s) to scan for files
    paths: Optional[List[str]]
    # new-line-delimited text file containing path(s) to scan for files
    paths_file: str
    # site value of the "locations" object (WIPAC, NERSC, etc.)
    site: str


class OAuthConfiguration(TypedDict):
    # The OAuth server URL for OpenID discovery
    oauth_url: str
    # The OAuth client id
    oauth_client_id: str
    # The OAuth client secret, to enable client credential mode
    oauth_client_secret: Optional[str]


class RestConfiguration(TypedDict):
    # URL for File Catalog REST API
    file_catalog_rest_url: str
    # URL for IceProd REST API
    iceprod_rest_url: str
    # request will timeout after this many seconds
    rest_timeout: int
    # number of retries to attempt
    rest_retries: int
