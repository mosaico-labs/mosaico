from enum import Enum

from deprecated import deprecated


@deprecated(
    "OnErrorPolicy is deprecated since v0.3.0; use SessionLevelErrorPolicy instead. "
    "It will be removed in v0.4.0."
)
class OnErrorPolicy(Enum):
    """
    Defines the behavior of the [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter]
    when an exception occurs during ingestion.

    This policy determines how the platform handles partially uploaded data if the
    ingestion process is interrupted or fails.
    """

    Report = "report"
    """
    Notify the server of the error but retain partial data.
    
    The system will attempt to finalize the sequence and notify the server of the 
    specific failure, allowing existing data chunks to remain accessible for 
    inspection. 

    Note:
        When the connection is established via the authorization middleware 
        (i.e. using an API Key), this policy requires the minimum
        [`APIKeyPermissionEnum.Read`][mosaicolabs.enum.APIKeyPermissionEnum.Read]
        permission.
    
    Important: Lock Status
        Unlike standard successful finalization, a sequence finalized via a 
        `Report` policy is **not placed in a locked state**. 
        This means the sequence remains mutable at a system level and can be 
        **deleted in a later moment** once debugging or triage is complete.
    """

    Delete = "delete"
    """
    Delete the sequence and instruct the server to discard all data.

    This is the default "all-or-nothing" strategy. If a failure occurs, the 
    [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] will send an abort 
    command to ensure the server purges all traces of the failed ingestion, 
    preventing inconsistent or incomplete sequences from appearing in the 
    catalog.
    
    Note:
        When the connection is established via the authorization middleware 
        (i.e. using an API Key), this policy is successfully executed by the 
        server only if the API Key has [`APIKeyPermissionEnum.Delete`][mosaicolabs.enum.APIKeyPermissionEnum.Delete]
        permission.
    
    """
