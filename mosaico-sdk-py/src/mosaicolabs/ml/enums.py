from enum import StrEnum


class SynchPolicy(StrEnum):
    """
    Defines the discrete temporal synchronization strategies for the MosaicoSyncTransformer.

    These policies determine how sparse, non-aligned sensor data is mapped onto
    a fixed-frequency temporal grid.
    """

    Hold = "hold"
    """
    Classic Last-Value-Hold. 
    
    Carries forward the most recent valid sample indefinitely until a new one arrives. 
    If a tick occurs before the first physical acquisition of a sensor, it remains 
    None to maintain semantic honesty.
    """

    AsOf = "as_of"
    """
    Tolerance-based Hold. 
    
    Yields the last known value only if the time delta between the sample and 
    the current grid tick is within a specified `tolerance_ns`. 
    If the gap exceeds this limit, the data is considered stale and results in None.
    """

    Drop = "drop"
    """
    Interval-based Hold (Windowed). 
    
    Strict synchronization that only returns a value if a sample arrived within 
     the specific interval (t - step_ns, t], where 't' is the current tick and 
    'step_ns' is the grid period. Effectively "drops" the signal during periods 
    of sensor silence.
    """
