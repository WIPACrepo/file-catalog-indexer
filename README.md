# file-catalog-indexer
Indexing scripts for the file catalog

[![CircleCI](https://circleci.com/gh/WIPACrepo/file-catalog-indexer/tree/master.svg?style=shield)](https://circleci.com/gh/WIPACrepo/file-catalog-indexer/tree/master) <sup>(master branch)</sup>

## Metadata Schema
see: [Google Doc](https://docs.google.com/document/d/14SanUWiYEbgarElt0YXSn_2We-rwT-ePO5Fg7rrM9lw/edit?usp=sharing)
also: [File Catalog Types]https://github.com/WIPACrepo/file_catalog/blob/master/file_catalog/schema/types.py

## Warnings

### Re-indexing Files is Tricky (Two Scenarios)
A. Indexing files that have not changed locations is okay--this probably means that the rest of the metadata has also not changed. A guardrail query will check if the file exists in the FC with that `locations` entry, and will not process the file further.
B. HOWEVER, don't point the indexer at restored files (of the same file-version)--those that had their initial `locations` entry removed (ie. removed from WIPAC, then moved back). Unlike re-indexing an unchanged file, this file will be *fully locally processed* (opened, read, and check-summed) before encountering the checksum-conflict then aborting. These files will be skipped (not sent to FC), unless you use `--patch` *(replaces the `locations` list, wholesale)*, which is **DANGEROUS**.
	- Example Conflict: It's possible a file-version exists in FC after initial guardrails
		1. file was at WIPAC & indexed
		2. then moved to NERSC (`location` added) & deleted from WIPAC (`location` removed)
		3. file was brought back to WIPAC
		4. now is being re-indexed at WIPAC
		5. CONFLICT -> has the same `logical_name`+`checksum.sha512` but differing `locations`