<!--- Top of README Badges (automated) --->
[![PyPI](https://img.shields.io/pypi/v/wipac-file-catalog-indexer)](https://pypi.org/project/wipac-file-catalog-indexer/) [![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/WIPACrepo/file-catalog-indexer?include_prereleases)](https://github.com/WIPACrepo/file-catalog-indexer/) [![PyPI - License](https://img.shields.io/pypi/l/wipac-file-catalog-indexer)](https://github.com/WIPACrepo/file-catalog-indexer/blob/master/LICENSE) [![Lines of code](https://img.shields.io/tokei/lines/github/WIPACrepo/file-catalog-indexer)](https://github.com/WIPACrepo/file-catalog-indexer/) [![GitHub issues](https://img.shields.io/github/issues/WIPACrepo/file-catalog-indexer)](https://github.com/WIPACrepo/file-catalog-indexer/issues?q=is%3Aissue+sort%3Aupdated-desc+is%3Aopen) [![GitHub pull requests](https://img.shields.io/github/issues-pr/WIPACrepo/file-catalog-indexer)](https://github.com/WIPACrepo/file-catalog-indexer/pulls?q=is%3Apr+sort%3Aupdated-desc+is%3Aopen) 
<!--- End of README Badges (automated) --->
# file-catalog-indexer
Indexing package and scripts for the File Catalog

## How To

### API
#### `from indexer.index import index`
- The flagship indexing function
- Find files rooted at given path(s), compute their metadata, and upload it to File Catalog
- Configurable for multi-processing (default: 1 process) and recursive file-traversing (default: on)
- Internally communicates asynchronously with File Catalog
- Note: Symbolic links are never followed.
- Note: `index()` runs the current event loop (`asyncio.get_event_loop().run_until_complete()`)
- Ex:
```python
index(
	index_config,  # see config.py for a description of the fields in these typed dictionaries
	oauth_config,
	rest_config
)
```

#### `from indexer.index import index_file`
- Compute metadata of a single file, and upload it to File Catalog, i.e. index one file
- Single-processed, single-threaded
```python
await index_file(
    filepath='/data/exp/IceCube/2018/filtered/level2/0820/Run00131410_74/Level2_IC86.2018_data_Run00131410_Subrun00000000_00000172.i3.zst',
    manager=MetadataManager(...),
    fc_rc=RestClient(...),
)
```

#### `from indexer.index import index_paths`
- A wrapper around `index_file()` which indexes multiple files, and returns any nested sub-directories
- Single-processed, single-threaded
- Note: Symbolic links are never followed.
```python
sub_dirs = await index_paths(
    paths=['/data/exp/IceCube/2018/filtered/level2/0820', '/data/exp/IceCube/2018/filtered/level2/0825'],
    manager=MetadataManager(...),
    fc_rc=RestClient(...),
)
```

#### `from indexer.metadata_manager import MetadataManager`
- The internal brain of the Indexer. This has minimal guardrails, does not communicate to File Catalog, and does not traverse file directory tree.
- Metadata is produced for an individual file, at a time.
- Ex:
```python
manager = MetadataManager(...)  # caches connections & directory info, manages metadata collection
metadata_file = manager.new_file(filepath)  # returns an instance (computationally light)
metadata = metadata_file.generate()  # returns a dict (computationally intense)
 ```

### Scripts
##### `python -m indexer.index`
- A command-line alternative to using `from indexer.index import index`
- Use with `-h` to see usage.
- Note: Symbolic links are never followed.

##### `python -m indexer.generate`
- Like `python -m indexer.index`, but prints (using `pprint`) the metadata instead of posting to File Catalog.
- Simply, uses file-traversing logic around calls to `indexer.metadata_manager.MetadataManager`
- Note: Symbolic links are never followed.

##### `python -m indexer.delocate`
- Find files rooted at given path(s); for each, remove the matching location entry from its File Catalog record.
- Note: Symbolic links are never followed.

## .i3 File Processing-Level Detection and Embedded Filename-Metadata Extraction
Regex is used heavily to detect the processing level of a `.i3` file, and extract any embedded metadata in the filename. The exact process depends on the type of data:

### Real Data (`/data/exp/*`)
This is a two-stage process (see `MetadataManager._new_file_real()`):
1. Processing-Level Detection (Base Pattern Screening)
	- The filename is applied to multiple generic patterns to detect if it is L2, PFFilt, PFDST, or PFRaw
	- If the filename does not trigger a match, *only basic metadata is collected* (`logical_name`, `checksum`, `file_size`, `locations`, and `create_date`)
2. Embedded Filename-Metadata Extraction
	- After the processing level is known, the filename is parsed using one of (possibly) several tokenizing regex patterns for the best match (greedy matching)
	- If the filename does not trigger a match, *the function will raise an exception (script will exit).* This probably indicates that a new pattern needs to be added to the list.
		+ see `indexer.metadata.real.filename_patterns`

### Simulation Data (`/data/sim/*`)
This is a three-stage process (see `MetadataManager._new_file_simulation()`):
1. Base Pattern Screening
	- The filename is checked for `.i3` file extensions: `.i3`, `.i3.gz`, `.i3.bz2`, `.i3.zst`
	- If the filename does not trigger a match, *only basic metadata is collected* (`logical_name`, `checksum`, `file_size`, `locations`, and `create_date`)
		+ there are a couple hard-coded "anti-patterns" used for rejecting known false-positives (see code)
2. Embedded Filename-Metadata Extraction
	- The filename is parsed using one of MANY (around a thousand) tokenizing regex patterns for the best match (greedy matching)
	- If the filename does not trigger a match, *the function will raise an exception (script will exit).* This probably indicates that a new pattern needs to be added to the list.
		+ see `indexer.metadata.sim.filename_patterns`
3. Processing-Level Detection
	- The filename is parsed for substrings corresponding to a processing level
		+ see `DataSimI3FileMetadata.figure_processing_level()`
	- If there is no match, `processing_level` will be set to `None`, since the processing level is less important for simulation data.


## Metadata Schema
See:
- [Google Doc](https://docs.google.com/document/d/14SanUWiYEbgarElt0YXSn_2We-rwT-ePO5Fg7rrM9lw/edit?usp=sharing)
- [File Catalog Types](https://github.com/WIPACrepo/file_catalog/blob/master/file_catalog/schema/types.py)


## Warnings

### Re-indexing Files is Tricky (Two Scenarios)
1. Indexing files that have not changed locations is okay--this probably means that the rest of the metadata has also not changed. A guardrail query will check if the file exists in the FC with that `locations` entry, and will not process the file further.
2. HOWEVER, don't point the indexer at restored files (of the same file-version)--those that had their initial `locations` entry removed (ie. removed from WIPAC, then moved back). Unlike re-indexing an unchanged file, this file will be *fully locally processed* (opened, read, and check-summed) before encountering the checksum-conflict then aborting. These files will be skipped (not sent to FC), unless you use `--patch` *(replaces the `locations` list, wholesale)*, which is **DANGEROUS**.
	- Example Conflict: It's possible a file-version exists in FC after initial guardrails
		1. file was at WIPAC & indexed
		2. then moved to NERSC (`location` added) & deleted from WIPAC (`location` removed)
		3. file was brought back to WIPAC
		4. now is being re-indexed at WIPAC
		5. CONFLICT -> has the same `logical_name`+`checksum.sha512` but differing `locations`