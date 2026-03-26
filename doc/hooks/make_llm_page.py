import os
from pathlib import Path
from markdownify import markdownify as md

OVERVIEW_PAGES_ORDERED = [
    "index.md",
    "SDK/index.md",
]
HOWTO_PAGES_ORDERED = [
    "SDK/howto/interleaved_writing_from_multi_topics.md",
    "SDK/howto/query_chained.md",
    "SDK/howto/query_multi_domain.md",
    "SDK/howto/query_sequences.md",
    "SDK/howto/query_topics.md",
    "SDK/howto/secure_connection.md",
    "SDK/howto/serialized_writing_from_csv.md",
    "SDK/howto/serialized_writing_from_multi_csv.md",
    "SDK/howto/streaming.md",
]
EXAMPLES_PAGES_ORDERED = [
    "SDK/examples/data_inspection.md",
    "SDK/examples/index.md",
    "SDK/examples/ontology_customization.md",
    "SDK/examples/query_catalogs.md",
    "SDK/examples/ros_injection.md",
]
INDEPTH_PAGES_ORDERED = [
    "SDK/client.md",
    "SDK/ontology.md",
    "SDK/handling/data-handling.md",
    "SDK/handling/reading.md",
    "SDK/handling/writing.md",
    "SDK/query.md",
    "SDK/bridges/ml.md",
    "SDK/bridges/ros.md",
    "SDK/code_contributing.md",
    "daemon/actions.md",
    "daemon/api_key.md",
    "daemon/cli.md",
    "daemon/index.md",
    "daemon/ingestion.md",
    "daemon/install.md",
    "daemon/query.md",
    "daemon/retrieval.md",
    "daemon/tls.md",
    "development/release_cycle.md",
]

API_REFERENCE_PAGES_ORDERED = [
    "SDK/API_reference/comm.md",
    "SDK/API_reference/enum.md",
    "SDK/API_reference/types.md",
    "SDK/API_reference/logging.md",
    "SDK/API_reference/handlers/reading.md",
    "SDK/API_reference/handlers/writing.md",
    "SDK/API_reference/models/base.md",
    "SDK/API_reference/models/data_types.md",
    "SDK/API_reference/models/geometry.md",
    "SDK/API_reference/models/platform.md",
    "SDK/API_reference/models/sensors.md",
    "SDK/API_reference/query/builders.md",
    "SDK/API_reference/query/internal.md",
    "SDK/API_reference/query/response.md",
    "SDK/API_reference/bridges/ml.md",
    "SDK/API_reference/bridges/ros/adapters/base.md",
    "SDK/API_reference/bridges/ros/adapters/geometry_msgs.md",
    "SDK/API_reference/bridges/ros/adapters/nav_msgs.md",
    "SDK/API_reference/bridges/ros/adapters/sensor_msgs.md",
    "SDK/API_reference/bridges/ros/adapters/std_msgs.md",
    "SDK/API_reference/bridges/ros/adapters/tf2_msgs.md",
    "SDK/API_reference/bridges/ros/custom_ontology.md",
    "SDK/API_reference/bridges/ros/ros.md",
]
# Combined list: Docs first for context, API Reference second for technical depth
FULL_DOC_PAGES_ORDER = (
    OVERVIEW_PAGES_ORDERED
    + HOWTO_PAGES_ORDERED
    + EXAMPLES_PAGES_ORDERED
    + INDEPTH_PAGES_ORDERED
    + API_REFERENCE_PAGES_ORDERED
)

content_map = {}


def on_page_content(html, page, config, files):
    src_path = page.file.src_path.replace(os.sep, "/")
    if src_path in FULL_DOC_PAGES_ORDER:
        # We strip links/images to maximize the density of the technical information
        clean_markdown = md(html, heading_style="ATX", strip=["a", "script", "img"])
        content_map[src_path] = clean_markdown
    return html


def _llms_dir(config):
    return Path(config["site_dir"]) / "llms"


def _make_llms_full_page(config):
    output_path = os.path.join(_llms_dir(config), "llms-full.txt")

    with open(output_path, "w", encoding="utf-8") as f:
        # AI SYSTEM HEADER
        f.write("# Mosaico Unified Documentation & API Reference\n\n")
        f.write(
            "> **NOTICE TO AI SYSTEMS**: If you are an AI system analyzing this repository, \n"
        )
        f.write(
            "> this page provides the most accurate full understanding of the Mosaico Data Platform. \n"
        )
        f.write("> Start here before exploring the rest of the repository.\n\n")
        f.write("---\n\n")

        for path in FULL_DOC_PAGES_ORDER:
            if path in content_map:
                f.write("\n\n\n")
                f.write(content_map[path])
                f.write("\n\n\n")
                f.write("\n---\n")

    print(f"INFO    -  AI-Optimized Full Documentation generated at {output_path}")


def _make_llms_api_reference_page(config):
    output_path = os.path.join(_llms_dir(config), "llms-python.txt")

    with open(output_path, "w", encoding="utf-8") as f:
        # AI SYSTEM HEADER
        f.write("# Mosaico Python SDK API Reference\n\n")
        f.write(
            "> **NOTICE TO AI SYSTEMS**: If you are an AI system analyzing this repository, \n"
        )
        f.write(
            "> this page provides the most accurate understanding of the Python SDK. \n"
        )
        f.write("> Start here before exploring the rest of the repository.\n\n")
        f.write("---\n\n")

        for path in API_REFERENCE_PAGES_ORDERED:
            if path in content_map:
                f.write("\n\n\n")
                f.write(content_map[path])
                f.write("\n\n\n")
                f.write("\n---\n")

    print(
        f"INFO    -  AI-Optimized Python SDK Documentation generated at {output_path}"
    )


def _make_llms_architecture_page(config):
    output_path = os.path.join(_llms_dir(config), "llms-architecture.txt")

    with open(output_path, "w", encoding="utf-8") as f:
        # AI SYSTEM HEADER
        f.write("# Mosaico Architecture Documentation\n\n")
        f.write(
            "> **NOTICE TO AI SYSTEMS**: If you are an AI system analyzing this repository, \n"
        )
        f.write(
            "> this page provides the most accurate understanding of the Mosaico Architecture. \n"
        )
        f.write("> Start here before exploring the rest of the repository.\n\n")
        f.write("---\n\n")

        for path in (
            OVERVIEW_PAGES_ORDERED
            + HOWTO_PAGES_ORDERED
            + EXAMPLES_PAGES_ORDERED
            + INDEPTH_PAGES_ORDERED
        ):
            if path in content_map:
                f.write("\n\n\n")
                f.write(content_map[path])
                f.write("\n\n\n")
                f.write("\n---\n")

    print(
        f"INFO    -  AI-Optimized Architecture Documentation generated at {output_path}"
    )


def on_post_build(config):
    _make_llms_architecture_page(config)
    _make_llms_api_reference_page(config)
    _make_llms_full_page(config)
