"""Type hints."""

from typing import List, Optional, TypedDict

Date = str
EventID = int


class Checksum(TypedDict, total=False):
    """Checksum dict."""

    sha512: str


class LocationEntry(TypedDict, total=False):
    """Location entry."""

    site: str
    path: str
    archive: bool


class SoftwareEntry(TypedDict, total=False):
    """Software entry."""

    name: str
    version: str
    date: Date


class Run(TypedDict):
    """Run dict."""

    run_number: int
    subrun_number: int
    part_number: int
    start_datetime: Optional[Date]  # ISO date
    end_datetime: Optional[Date]  # ISO date
    first_event: Optional[EventID]
    last_event: Optional[EventID]
    event_count: int


class GapEntry(TypedDict):
    """Gap dict."""

    start_event_id: EventID
    stop_event_id: EventID
    delta_time: int
    start_date: Date
    stop_date: Date


class Event(TypedDict):
    """Event entry."""

    event_id: EventID
    datetime: Date


class OfflineProcessingMetadata(TypedDict, total=False):
    """Offline Processing Metadata."""

    dataset_id: int
    season: Optional[int]
    season_name: Optional[str]
    L2_gcd_file: str
    L2_snapshot_id: int
    L2_production_version: int
    L3_source_dataset_id: int
    working_group: str
    validation_validated: bool
    validation_date: Date
    validation_software: SoftwareEntry
    livetime: Optional[float]
    gaps: Optional[List[GapEntry]]
    first_event: Optional[Event]
    last_event: Optional[Event]


class Metadata(TypedDict, total=False):
    """The file-catalog metadata.

    https://docs.google.com/document/d/14SanUWiYEbgarElt0YXSn_2We-rwT-ePO5Fg7rrM9lw/view#heading=h.yq8ukujsb797
    """

    # Basic File:
    logical_name: str
    locations: List[LocationEntry]
    file_size: int
    checksum: Checksum
    create_date: Date

    # i3 File:
    meta_modify_date: Date
    data_type: Optional[str]
    processing_level: Optional[str]
    content_status: str
    software: Optional[List[SoftwareEntry]]
    run: Run

    # Offline Processing:
    offline_processing_metadata: OfflineProcessingMetadata
