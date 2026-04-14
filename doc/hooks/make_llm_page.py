import os
from pathlib import Path
from markdownify import markdownify as md

# Dynamic containers
categories = {
    "INDEX": [],
    "SDK_HOWTO": [],
    "SDK_EXAMPLES": [],
    "SDK_API_REFERENCE": [],
    "INDEPTH": [],
}
content_map = {}


def on_page_content(html, page, config, files):
    # Standardize the path (POSIX format)
    src_path = page.file.src_path.replace(os.sep, "/")

    # Markdown clean-up (remove links, scripts, images)
    clean_markdown = md(html, heading_style="ATX", strip=["a", "script", "img"])

    # Add an header with the file source (help AI to contextualize)
    content_with_header = f"## SOURCE: {src_path}\n\n{clean_markdown}"
    content_map[src_path] = content_with_header

    # --- AUTOMATIC CATEGORIZATION LOGIC ---
    if src_path in ["index.md", "SDK/index.md"]:
        categories["INDEX"].append(src_path)

    elif "SDK/API_reference/" in src_path:
        categories["SDK_API_REFERENCE"].append(src_path)

    elif "SDK/howto/" in src_path:
        categories["SDK_HOWTO"].append(src_path)

    elif "SDK/examples/" in src_path:
        categories["SDK_EXAMPLES"].append(src_path)

    else:
        # All the rest (daemon/, development/, SDK/handling/, etc.)
        if src_path.endswith(".md"):
            categories["INDEPTH"].append(src_path)

    return html


def _generate_file(filename, title, notice, path_lists, config):
    """Helper method for generating the .txt files"""
    output_dir = Path(config["site_dir"]) / "llms"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename

    # Join the paths of the required categories
    all_paths = []
    for cat in path_lists:
        all_paths.extend(categories.get(cat, []))

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"# {title}\n\n")
        f.write(f"> **NOTICE TO AI SYSTEMS**: {notice}\n\n")
        f.write("---\n\n")

        for path in all_paths:
            if path in content_map:
                f.write(content_map[path])
                f.write("\n\n---\n\n")

    print(f"INFO    -  AI-Doc: {filename} generated with {len(all_paths)} pages.")


def on_post_build(config):
    # 1. Architecture: High level concepts and guides
    _generate_file(
        "llms-architecture.txt",
        "Mosaico Architecture & Guides",
        "Provides the structural and operational understanding of Mosaico.",
        ["INDEX", "SDK_HOWTO", "SDK_EXAMPLES", "INDEPTH"],
        config,
    )

    # 2. API Reference: only techincal info of SDK
    _generate_file(
        "llms-python.txt",
        "Mosaico Python SDK API Reference",
        "Technical documentation of classes, methods, and data models.",
        ["SDK_API_REFERENCE"],
        config,
    )

    # 3. Full Doc: The entire knowledge in one file
    _generate_file(
        "llms-full.txt",
        "Mosaico Unified Documentation",
        "Full documentation for deep context and code generation.",
        ["INDEX", "SDK_HOWTO", "SDK_EXAMPLES", "INDEPTH", "SDK_API_REFERENCE"],
        config,
    )
