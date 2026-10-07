"""
Helpers used by the Watchdog to build the injector of a file and the name of its sequence.
"""

import re
from pathlib import Path
from typing import Optional, Type

from mosaicolabs import SessionLevelErrorPolicy, TopicLevelErrorPolicy
from mosaicolabs.bridges.mcap import MCAPInjectionConfig, MCAPInjector
from mosaicolabs.bridges.ros import RosbagInjector, ROSInjectionConfig

from .file_source import FileRef

_UNSUPPORTED_SEQUENCE_NAME_CHARS = re.compile(r"[^A-Za-z0-9_-]")
"""Characters the SDK does not accept in a sequence name (see `_validate_sequence_name`)."""


def create_ros_configs(
    path: Path,
    sequence_name: str,
    host: str,
    port: int,
    mosaico_api_key: Optional[str],
    tls_cert_path: Optional[str],
    enable_tls: bool,
    log_level: str,
) -> ROSInjectionConfig:
    """
    Builds the `ROSInjectionConfig` the Watchdog uses to inject a ROS bag file.
    """
    return ROSInjectionConfig(
        file_path=path,
        sequence_name=sequence_name,
        metadata={},
        topic_metadata=None,
        update_if_exists=False,
        host=host,
        port=port,
        on_error=SessionLevelErrorPolicy.Delete,
        topics_on_error=TopicLevelErrorPolicy.Raise,
        serialization_formats=None,
        log_level=log_level,
        mosaico_api_key=mosaico_api_key,
        tls_cert_path=tls_cert_path,
        enable_tls=enable_tls,
        dry_run=False,
    )


def create_mcap_configs(
    path: Path,
    sequence_name: str,
    host: str,
    port: int,
    mosaico_api_key: Optional[str],
    tls_cert_path: Optional[str],
    enable_tls: bool,
    log_level: str,
) -> MCAPInjectionConfig:
    """
    Builds the `MCAPInjectionConfig` the Watchdog uses to inject an MCAP file, with the same
    settings as `create_ros_configs()`.
    """
    return MCAPInjectionConfig(
        file_path=path,
        sequence_name=sequence_name,
        metadata={},
        topic_metadata=None,
        update_if_exists=False,
        host=host,
        port=port,
        on_error=SessionLevelErrorPolicy.Delete,
        topics_on_error=TopicLevelErrorPolicy.Raise,
        serialization_formats=None,
        log_level=log_level,
        mosaico_api_key=mosaico_api_key,
        tls_cert_path=tls_cert_path,
        enable_tls=enable_tls,
        dry_run=False,
    )


def instantiate_injector(
    path: Path,
    sequence_name: str,
    host: str,
    port: int,
    mosaico_api_key: Optional[str],
    tls_cert_path: Optional[str],
    enable_tls: bool,
    log_level: str,
    injector_cls: Type[RosbagInjector] | Type[MCAPInjector],
) -> RosbagInjector | MCAPInjector:
    """
    Creates an injector of class `injector_cls`, with the config built for it by
    `create_ros_configs()` or `create_mcap_configs()`.
    """
    if injector_cls is RosbagInjector:
        configs = create_ros_configs(
            path=path,
            sequence_name=sequence_name,
            host=host,
            port=port,
            mosaico_api_key=mosaico_api_key,
            tls_cert_path=tls_cert_path,
            enable_tls=enable_tls,
            log_level=log_level,
        )
        return injector_cls(configs)
    elif injector_cls is MCAPInjector:
        configs = create_mcap_configs(
            path=path,
            sequence_name=sequence_name,
            host=host,
            port=port,
            mosaico_api_key=mosaico_api_key,
            tls_cert_path=tls_cert_path,
            enable_tls=enable_tls,
            log_level=log_level,
        )
        return injector_cls(configs)
    else:
        raise RuntimeError("TODO")


def sequence_name_from_fileref(file_ref: FileRef) -> str:
    """
    Builds the sequence name of a file from its relative path.

    Folders are joined with `-`, then come `__` and the extension: `run_01/drive.mcap`
    becomes `run_01-drive__mcap`. Characters the SDK does not accept in a sequence name
    (anything but letters, digits, `-` and `_`) become `_`, and leading `-`/`_` are dropped
    so that the name starts with a letter or a digit.

    Different paths can still give the same name (e.g. `a-b/c.mcap` and `a/b-c.mcap`): the
    second file then fails because the sequence already exists.

    Args:
        file_ref (FileRef): The file to name.

    Returns:
        str: The sequence name.
    """
    # The last `.` separates the extension: LocalFileSource only lists files that have one
    stem, _, extension = file_ref.relative_path.rpartition(".")
    name = f"{stem.replace('/', '-')}__{extension}"

    return _UNSUPPORTED_SEQUENCE_NAME_CHARS.sub("_", name).lstrip("-_")
