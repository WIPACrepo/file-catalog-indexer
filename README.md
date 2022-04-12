# file-catalog-indexer
Indexing package and scripts for the File Catalog

## How To

### As an Imported Package
#### `from indexer.index import index`
- The flagship indexing function
- Find files rooted at given path(s), compute their metadata, and upload it to File Catalog.
- Configurable for multi-processing, multi-threading, recursive file-traversing
- Note: Symbolic links are never followed.
- Note: `index()` runs the current event loop (`asyncio.get_event_loop().run_until_complete()`)
- Ex:
```python
index(
    fc_token,
    'WIPAC',
    paths=['/data/exp/IceCube/2018/filtered/level2/0820', '/data/exp/IceCube/2018/filtered/level2/0825'],
    blacklist=['/data/exp/IceCube/2018/filtered/level2/0820/Run00131410_74'],
    n_processes=4,
)
 ```

### `from indexer.index import index_file`
- Compute metadata of a single file, and upload it to File Catalog, i.e. index one file
- Single-processed, single-threaded
```python
await index_file(
    filepath='/data/exp/IceCube/2018/filtered/level2/0820/Run00131410_74/Level2_IC86.2018_data_Run00131410_Subrun00000000_00000172.i3.zst',
    manager=MetadataManager(...),
    fc_rc=RestClient(...),
)
```

### `from indexer.index import index_paths`
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
metadata_file = manager.new_file(filepath)  # returns an instance
metadata = metadata_file.generate()  # returns a dict
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


## Metadata Schema
see: [Google Doc](https://docs.google.com/document/d/14SanUWiYEbgarElt0YXSn_2We-rwT-ePO5Fg7rrM9lw/edit?usp=sharing)
also: [File Catalog Types]https://github.com/WIPACrepo/file_catalog/blob/master/file_catalog/schema/types.py


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