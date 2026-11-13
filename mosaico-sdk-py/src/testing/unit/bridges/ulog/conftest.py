import struct
from pathlib import Path

import numpy as np
import pytest
from pyulog import ULog

FORMATS = [
    # Nested format: its `timestamp` is a regular field, its padding ends up inside
    # the parent rows
    "report:uint64_t timestamp;int16_t[2] x;uint8_t[4] _padding0;",
    "status:uint64_t timestamp;char[4] name;bool ok;report[2] reports;uint8_t[3] _padding0;",
    "baro:uint64_t timestamp;float p;",
    "gps:uint64_t timestamp;uint64_t time_utc_usec;",
    "preflight:uint64_t timestamp;float v;",
    "heartbeat:uint64_t timestamp;uint8_t[4] _padding0;",
]


def _status_row(timestamp: int, name: bytes, ok: bool) -> tuple:
    """A `status` row as pyulog lays it out: flat, nested fields and paddings included."""
    reports = [(timestamp + i, 10 * i, -10 * i, 0, 0, 0, 0) for i in (1, 2)]
    return (timestamp, *name.ljust(4, b"\0"), ok, *reports[0], *reports[1])


SUBSCRIPTIONS = [
    # (format, multi_id, rows); the position in the list is the msg_id.
    # One row without timestamp (dropped) and two rows with the same one (both kept)
    (
        "status",
        0,
        [
            _status_row(0, b"zero", False),
            _status_row(1000, b"ab", True),
            _status_row(1000, b"abcd", False),
            _status_row(2000, b"", True),
        ],
    ),
    # The same topic subscribed twice: its rows have to be merged and sorted
    ("baro", 0, [(100, 100.0), (300, 300.0)]),
    ("baro", 0, [(200, 200.0)]),
    # A second instance of the format, large enough to be cut into several chunks
    ("baro", 1, [(10_000 + 10 * i, float(i)) for i in range(1000)]),
    # `time_utc_usec` is 0 until the receiver knows the time
    ("gps", 0, [(400, 0), (500, 1_600_000_000_000_500)]),
    # No row with a valid timestamp
    ("preflight", 0, [(0, 1.0), (0, 2.0)]),
    # No payload field
    ("heartbeat", 0, [(600,), (700,)]),
]


def make_ulog(path: Path) -> Path:
    """
    Writes a small `.ulg` file with `ULog.write_ulog`, holding the cases the bridge has to
    handle: equal and zero timestamps, a topic subscribed twice, two instances of a
    format, a nested format in an array, `char[N]`, `bool`, top-level and nested padding,
    a topic without valid timestamps, one without payload, a GPS time, logged messages
    (tagged and not), parameters changed in flight, a dropout, info records and initial
    and default parameters.

    `write_ulog` sorts the records of the data section by timestamp, so decreasing
    timestamps within a subscription cannot be written. Dropouts and changed parameters
    have no time of their own: the one given here only places them in the file, and
    pyulog gives them the timestamp of the last data record read before them (500 and
    200 here).
    """
    ulog = ULog(None)  # an empty log, filled by hand below

    for definition in FORMATS:
        message_format = ULog.MessageFormat(definition.encode(), None)
        ulog.message_formats[message_format.name] = message_format

    for msg_id, (name, multi_id, rows) in enumerate(SUBSCRIPTIONS):
        # NOTE: pyulog offers no public way to build a `Data` from rows: this private
        # class works out the flat layout of the format, as it does when reading a file
        subscription = ULog._MessageAddLogged(
            struct.pack("<BH", multi_id, msg_id) + name.encode(),
            None,
            ulog.message_formats,
        )
        subscription.buffer = np.array(rows, subscription.dtype).tobytes()
        ulog.data_list.append(ULog.Data(subscription))

    ulog.logged_messages.append(
        ULog.MessageLogging(struct.pack("<BQ", 54, 150) + b"untagged", None)
    )
    ulog.logged_messages_tagged[7] = [
        ULog.MessageLoggingTagged(struct.pack("<BHQ", 52, 7, 50) + b"tagged", None)
    ]
    ulog.dropouts.append(ULog.MessageDropout(struct.pack("<H", 12), None, 550))
    ulog.changed_parameters.extend([(250, "P_INT", 3), (260, "P_FLT", 0.5)])

    # NOTE: the types of the info records and the default parameters have no public
    # setter either
    ulog.msg_info_dict.update(sys_name="PX4", ver_hw="TEST", time_ref_utc=3600)
    ulog._msg_info_dict_types.update(
        sys_name="char[3]", ver_hw="char[4]", time_ref_utc="int32_t"
    )
    ulog.msg_info_multiple_dict["boot_console_output"] = [["first ", "second"]]
    ulog._msg_info_multiple_dict_types["boot_console_output"] = "char[6]"
    ulog.initial_parameters.update(P_INT=1, P_FLT=0.25)
    ulog._default_parameters.update({0: {"P_INT": 0}, 1: {"P_FLT": 0.5}})
    ulog._compat_flags[0] |= 1  # the file has default parameters

    ulog.write_ulog(str(path))
    return path


@pytest.fixture(scope="session")
def ulog_file(tmp_path_factory) -> Path:
    """The path of a generated `.ulg` file (see `make_ulog`)."""
    return make_ulog(tmp_path_factory.mktemp("data") / "flight.ulg")
