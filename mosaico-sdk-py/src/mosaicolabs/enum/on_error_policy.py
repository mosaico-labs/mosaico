from enum import Enum


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

    Important: Lock Status
        Unlike standard successful finalization, a sequence finalized via a 
        `Report` policy is **not placed in a locked state**. 
        This means the sequence remains mutable at a system level and can be 
        **deleted in a later moment** once debugging or triage is complete.
    """

    Delete = "delete"
    """
    Abort the sequence and instruct the server to discard all data.
    
    This is the default "all-or-nothing" strategy. If a failure occurs, the 
    [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] will send an abort 
    command to ensure the server purges all traces of the failed ingestion, 
    preventing inconsistent or incomplete sequences from appearing in the 
    catalog.
    """
