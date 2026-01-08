"""
Helper Utilities.

Provides utility functions for path manipulation, exception chaining,
and Flight ticket parsing.
"""

from pathlib import Path
from typing import Optional
import pyarrow.flight as fl
from ..helpers import unpack_topic_full_path


def _make_exception(msg: str, exc_msg: Optional[Exception] = None) -> Exception:
    """
    Creates a new exception that chains an inner exception's message.
    Useful for adding context to low-level Flight errors.

    Args:
        msg (str): The high-level error message.
        exc_msg (Optional[Exception]): The original exception.

    Returns:
        Exception: A new exception combining both messages.
    """
    if exc_msg is None:
        return Exception(msg)
    else:
        return Exception(f"{msg}\nInner err: {exc_msg}")


def _parse_ep_ticket(ticket: fl.Ticket) -> Optional[tuple[str, str]]:
    """
    Decodes a Flight Ticket to extract sequence and topic identifiers.

    Args:
        ticket (fl.Ticket): The opaque ticket object from a FlightInfo endpoint.

    Returns:
        Optional[tuple[str, str]]: (sequence_name, topic_name) if successful, else None.
    """
    ticket_str = ticket.ticket.decode("utf-8")
    seq_topic_tuple = unpack_topic_full_path(ticket_str)
    if not seq_topic_tuple:
        return None
    return seq_topic_tuple


def _validate_sequence_name(name: str):
    nbase = Path(name)
    if nbase.is_absolute():
        nbase = nbase.relative_to("/")
    if "/" in str(nbase):
        raise ValueError(f"Invalid characters '/' in sequence name '{name}'")
