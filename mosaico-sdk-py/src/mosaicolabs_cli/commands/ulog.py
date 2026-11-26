import typer

from mosaicolabs_cli.utils.bridge import (
    DryRunOpt,
    InputFileArg,
    LogLevel,
    LogLevelOpt,
    MetadataOpt,
    SequenceNameOpt,
    TopicMetadataOpt,
    TopicsOpt,
    connection_kwargs,
    parse_json_option,
    require_profile,
    run_injector,
)


def inject(
    ctx: typer.Context,
    ulog_path: InputFileArg,
    sequence_name: SequenceNameOpt,
    topics: TopicsOpt = None,
    metadata: MetadataOpt = None,
    topic_metadata: TopicMetadataOpt = None,
    dry_run: DryRunOpt = False,
    log_level: LogLevelOpt = LogLevel.INFO,
) -> None:
    """
    Inject a PX4 ULog (.ulg) file into Mosaico, as one sequence.
    """
    # A dry run never connects, so it can run without a profile
    profile = ctx.obj if dry_run else require_profile(ctx)

    user_metadata = parse_json_option(metadata, "--metadata")
    user_topic_metadata = parse_json_option(topic_metadata, "--topic-metadata")

    from mosaicolabs.bridges.ulog import ULogInjectionConfig, ULogInjector

    config = ULogInjectionConfig(
        file_path=ulog_path,
        sequence_name=sequence_name,
        metadata=user_metadata,
        topic_metadata=user_topic_metadata or None,
        dry_run=dry_run,
        topics=topics or None,
        log_level=log_level.value,
        **connection_kwargs(profile),
    )
    run_injector(ULogInjector(config))
