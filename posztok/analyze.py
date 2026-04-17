#!/usr/bin/env python3
"""
JSON Schema Analyzer & Documenter
Analyzes multiple JSON files and generates comprehensive schema documentation.
"""

import all_json
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Any


# ─────────────────────────────────────────────
#  CORE ANALYZER
# ─────────────────────────────────────────────

def get_type(value: Any) -> str:
    """Return a human-readable type name for a JSON value."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "unknown"


def analyze_value(value: Any, depth: int = 0, max_depth: int = 10) -> dict:
    """
    Recursively analyze a JSON value and return its schema info.
    """
    if depth > max_depth:
        return {"type": "unknown", "note": "max depth reached"}

    type_name = get_type(value)
    info = {"type": type_name}

    # ── String ──────────────────────────────
    if type_name == "string":
        info["length"]  = len(value)
        info["example"] = value[:80] + ("..." if len(value) > 80 else "")

    # ── Integer / Float ──────────────────────
    elif type_name in ("integer", "float"):
        info["value"] = value

    # ── Array ────────────────────────────────
    elif type_name == "array":
        info["length"] = len(value)
        if value:
            item_types = list({get_type(i) for i in value})
            info["item_types"] = item_types
            info["is_uniform"] = len(item_types) == 1
            # Analyze first non-null item as a sample
            sample = next((i for i in value if i is not None), value[0])
            info["sample_item"] = analyze_value(sample, depth + 1, max_depth)
        else:
            info["item_types"] = []
            info["is_uniform"] = True

    # ── Object ───────────────────────────────
    elif type_name == "object":
        info["field_count"] = len(value)
        info["fields"] = {
            k: analyze_value(v, depth + 1, max_depth)
            for k, v in value.items()
        }

    return info


def analyze_json_file(filepath: str, max_depth: int = 10) -> dict:
    """
    Load and fully analyze a JSON file.
    Returns a result dict (success or error).
    """
    result = {
        "file":      filepath,
        "filename":  os.path.basename(filepath),
        "analyzed":  datetime.now().isoformat(timespec="seconds"),
        "success":   False,
    }

    # ── Load ─────────────────────────────────
    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            raw   = fh.read()
            data  = all_json.loads(raw)
    except FileNotFoundError:
        result["error"] = f"File not found: {filepath}"
        return result
    except all_json.JSONDecodeError as exc:
        result["error"] = f"Invalid JSON – {exc}"
        return result
    except Exception as exc:
        result["error"] = f"Unexpected error – {exc}"
        return result

    # ── Meta ─────────────────────────────────
    result["file_size_bytes"] = os.path.getsize(filepath)
    result["root_type"]       = get_type(data)
    result["success"]         = True

    # ── Schema ───────────────────────────────
    result["schema"] = analyze_value(data, max_depth=max_depth)

    # ── Top-level stats ───────────────────────
    if isinstance(data, dict):
        result["top_level_keys"]  = list(data.keys())
        result["top_level_count"] = len(data)
    elif isinstance(data, list):
        result["array_length"]    = len(data)
        result["item_types"]      = list({get_type(i) for i in data})

    return result


# ─────────────────────────────────────────────
#  CROSS-FILE COMPARISON
# ─────────────────────────────────────────────

def compare_schemas(results: list[dict]) -> dict:
    """
    Compare schemas across multiple files.
    Highlights shared keys, unique keys, and type conflicts.
    """
    successful = [r for r in results if r.get("success")]
    if not successful:
        return {}

    object_files = [r for r in successful if r["root_type"] == "object"]
    if not object_files:
        return {"note": "No root-object files to compare."}

    # Collect keys per file
    key_sets = {r["filename"]: set(r.get("top_level_keys", [])) for r in object_files}
    all_keys = set().union(*key_sets.values())

    shared_keys = all_keys.copy()
    for ks in key_sets.values():
        shared_keys &= ks

    unique_keys: dict[str, list[str]] = defaultdict(list)
    for fname, ks in key_sets.items():
        for k in ks - shared_keys:
            unique_keys[k].append(fname)

    # Detect type conflicts on shared keys
    type_conflicts: dict[str, dict[str, str]] = {}
    for key in shared_keys:
        types_seen: dict[str, str] = {}
        for r in object_files:
            field_info = r["schema"].get("fields", {}).get(key, {})
            types_seen[r["filename"]] = field_info.get("type", "unknown")
        unique_types = set(types_seen.values())
        if len(unique_types) > 1:
            type_conflicts[key] = types_seen

    return {
        "files_compared":  [r["filename"] for r in object_files],
        "all_keys":        sorted(all_keys),
        "shared_keys":     sorted(shared_keys),
        "unique_keys":     dict(unique_keys),
        "type_conflicts":  type_conflicts,
        "consistency_pct": round(len(shared_keys) / len(all_keys) * 100, 1) if all_keys else 100.0,
    }


# ─────────────────────────────────────────────
#  FORMATTERS
# ─────────────────────────────────────────────

def _indent(level: int) -> str:
    return "  " * level


def format_schema_tree(schema: dict, indent: int = 0) -> str:
    """Recursively render a schema dict as a readable tree."""
    lines = []
    t = schema.get("type", "unknown")

    if t == "object":
        lines.append(f"{_indent(indent)}[object]  ({schema.get('field_count', 0)} fields)")
        for field_name, field_info in schema.get("fields", {}).items():
            lines.append(f"{_indent(indent + 1)}├─ {field_name}:")
            lines.append(format_schema_tree(field_info, indent + 2))

    elif t == "array":
        length     = schema.get("length", 0)
        item_types = schema.get("item_types", [])
        uniform    = schema.get("is_uniform", True)
        lines.append(
            f"{_indent(indent)}[array]  length={length}  "
            f"item_types={item_types}  uniform={uniform}"
        )
        if "sample_item" in schema:
            lines.append(f"{_indent(indent + 1)}└─ sample item:")
            lines.append(format_schema_tree(schema["sample_item"], indent + 2))

    elif t == "string":
        lines.append(
            f"{_indent(indent)}[string]  "
            f"length={schema.get('length', 0)}  "
            f"example=\"{schema.get('example', '')}\""
        )

    elif t in ("integer", "float"):
        lines.append(f"{_indent(indent)}[{t}]  value={schema.get('value', '')}")

    elif t == "boolean":
        lines.append(f"{_indent(indent)}[boolean]")

    elif t == "null":
        lines.append(f"{_indent(indent)}[null]")

    else:
        lines.append(f"{_indent(indent)}[{t}]")

    return "\n".join(lines)


def generate_text_report(results: list[dict], comparison: dict) -> str:
    """Build the full plain-text report."""
    SEP1 = "=" * 65
    SEP2 = "-" * 65

    lines = [
        SEP1,
        "  JSON SCHEMA ANALYSIS REPORT",
        f"  Generated : {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}",
        f"  Files     : {len(results)}",
        SEP1,
        "",
    ]

    # ── Per-file sections ─────────────────────
    for idx, result in enumerate(results, 1):
        lines += [
            f"FILE {idx}/{len(results)}: {result['filename']}",
            SEP2,
            f"  Path      : {result['file']}",
            f"  Analyzed  : {result.get('analyzed', 'N/A')}",
        ]

        if not result.get("success"):
            lines += [f"  ✗ ERROR   : {result.get('error', 'unknown')}", ""]
            continue

        lines += [
            f"  Size      : {result.get('file_size_bytes', 0):,} bytes",
            f"  Root type : {result.get('root_type', 'unknown')}",
        ]

        if result["root_type"] == "object":
            keys = result.get("top_level_keys", [])
            lines.append(f"  Keys ({len(keys)}): {', '.join(keys)}")
        elif result["root_type"] == "array":
            lines.append(f"  Length    : {result.get('array_length', 0):,}")
            lines.append(f"  Item types: {result.get('item_types', [])}")

        lines += ["", "  SCHEMA TREE", "  " + "-" * 40]
        lines.append(format_schema_tree(result["schema"], indent=1))
        lines.append("")

    # ── Comparison section ────────────────────
    if comparison and "files_compared" in comparison:
        lines += [
            SEP1,
            "  CROSS-FILE COMPARISON",
            SEP2,
            f"  Files compared : {len(comparison['files_compared'])}",
            f"  Total keys     : {len(comparison.get('all_keys', []))}",
            f"  Shared keys    : {len(comparison.get('shared_keys', []))}",
            f"  Consistency    : {comparison.get('consistency_pct', 0)}%",
            "",
            f"  Shared keys  : {', '.join(comparison.get('shared_keys', [])) or 'none'}",
        ]

        unique = comparison.get("unique_keys", {})
        if unique:
            lines.append("  Unique keys (key → files that have it):")
            for k, files in unique.items():
                lines.append(f"    • {k:30s} → {', '.join(files)}")
        else:
            lines.append("  Unique keys  : none")

        conflicts = comparison.get("type_conflicts", {})
        if conflicts:
            lines += ["", "  ⚠  TYPE CONFLICTS:"]
            for key, file_types in conflicts.items():
                lines.append(f"    • '{key}':")
                for fname, ftype in file_types.items():
                    lines.append(f"        {fname}: {ftype}")
        else:
            lines.append("  Type conflicts: none  ✓")

    lines += ["", SEP1]
    return "\n".join(lines)


def generate_json_report(results: list[dict], comparison: dict) -> str:
    """Serialize all analysis data as pretty JSON."""
    report = {
        "generated":  datetime.now().isoformat(timespec="seconds"),
        "file_count": len(results),
        "files":      results,
        "comparison": comparison,
    }
    return all_json.dumps(report, indent=2, default=str)


def generate_markdown_report(results: list[dict], comparison: dict) -> str:
    """Build a Markdown-formatted report."""
    lines = [
        "# JSON Schema Analysis Report",
        "",
        f"> **Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"> **Files analyzed:** {len(results)}",
        "",
    ]

    for idx, result in enumerate(results, 1):
        lines += [
            f"---",
            f"## {idx}. `{result['filename']}`",
            "",
            f"| Property | Value |",
            f"|----------|-------|",
            f"| Path | `{result['file']}` |",
            f"| Analyzed | {result.get('analyzed', 'N/A')} |",
        ]

        if not result.get("success"):
            lines += [
                f"| Status | ❌ Error |",
                f"| Error | {result.get('error', 'unknown')} |",
                "",
            ]
            continue

        lines += [
            f"| Size | {result.get('file_size_bytes', 0):,} bytes |",
            f"| Root type | `{result.get('root_type', 'unknown')}` |",
            f"| Status | ✅ OK |",
            "",
        ]

        if result["root_type"] == "object":
            keys = result.get("top_level_keys", [])
            lines.append(f"**Top-level keys ({len(keys)}):** "
                         + ", ".join(f"`{k}`" for k in keys))
        elif result["root_type"] == "array":
            lines.append(f"**Array length:** {result.get('array_length', 0):,}")

        lines += [
            "",
            "### Schema Tree",
            "```",
            format_schema_tree(result["schema"], indent=0),
            "```",
            "",
        ]

    # ── Comparison ────────────────────────────
    if comparison and "files_compared" in comparison:
        lines += [
            "---",
            "## Cross-File Comparison",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Files compared | {len(comparison['files_compared'])} |",
            f"| Total keys | {len(comparison.get('all_keys', []))} |",
            f"| Shared keys | {len(comparison.get('shared_keys', []))} |",
            f"| Consistency | **{comparison.get('consistency_pct', 0)}%** |",
            "",
        ]

        shared = comparison.get("shared_keys", [])
        if shared:
            lines.append("**Shared keys:** " + ", ".join(f"`{k}`" for k in shared))

        unique = comparison.get("unique_keys", {})
        if unique:
            lines += ["", "**Unique keys:**", ""]
            for k, files in unique.items():
                lines.append(f"- `{k}` → found only in: *{', '.join(files)}*")

        conflicts = comparison.get("type_conflicts", {})
        if conflicts:
            lines += ["", "### ⚠️ Type Conflicts", ""]
            for key, file_types in conflicts.items():
                lines.append(f"**`{key}`**")
                for fname, ftype in file_types.items():
                    lines.append(f"- `{fname}` → `{ftype}`")
                lines.append("")
        else:
            lines += ["", "✅ **No type conflicts detected.**", ""]

    return "\n".join(lines)


# ─────────────────────────────────────────────
#  OUTPUT
# ─────────────────────────────────────────────

def save_report(content: str, output_path: str) -> None:
    """Write report content to a file."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(content)
    print(f"  ✓ Report saved → {output_path}")


# ─────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Analyze and document multiple JSON file schemas.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python json_analyzer.py data/*.json
  python json_analyzer.py a.json b.json c.json --format markdown --output report.md
  python json_aanalyzer.py data/ --format all --output-dir ./reports
  python json_analyzer.py a.json --depth 5 --quiet
        """,
    )
    p.add_argument(
        "inputs",
        nargs="+",
        help="JSON files or directories to analyze",
    )
    p.add_argument(
        "--format", "-f",
        choices=["text", "json", "markdown", "all"],
        default="text",
        help="Output format (default: text)",
    )
    p.add_argument(
        "--output", "-o",
        default=None,
        help="Output file path (for single-format output)",
    )
    p.add_argument(
        "--output-dir", "-d",
        default=None,
        help="Output directory (used when --format all)",
    )
    p.add_argument(
        "--depth",
        type=int,
        default=10,
        help="Maximum recursion depth (default: 10)",
    )
    p.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress console output",
    )
    return p


def collect_json_files(inputs: list[str]) -> list[str]:
    """Expand directories and validate file paths."""
    files = []
    for inp in inputs:
        path = Path(inp)
        if path.is_dir():
            found = sorted(path.rglob("*.json"))
            files.extend(str(f) for f in found)
            print(f"  📁 Directory '{inp}': found {len(found)} JSON file(s)")
        elif path.is_file():
            files.append(str(path))
        else:
            print(f"  ⚠  Skipping '{inp}' – not a file or directory")
    return files


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────

def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    # ── Collect files ─────────────────────────
    print("\n🔍 JSON Schema Analyzer")
    print("=" * 40)
    json_files = collect_json_files(args.inputs)

    if not json_files:
        print("❌ No JSON files found. Exiting.")
        sys.exit(1)

    print(f"\n📄 Analyzing {len(json_files)} file(s)...\n")

    # ── Analyze ───────────────────────────────
    results = []
    for fp in json_files:
        print(f"  → {fp}")
        result = analyze_json_file(fp, max_depth=args.depth)
        results.append(result)
        status = "✓" if result.get("success") else "✗"
        print(f"    {status} {result.get('root_type', 'error')}"
              + (f" ({result.get('top_level_count', result.get('array_length', ''))} items)"
                 if result.get("success") else f": {result.get('error', '')}"))

    # ── Compare ───────────────────────────────
    comparison = compare_schemas(results)

    # ── Generate & output reports ─────────────
    fmt = args.format

    reports: dict[str, tuple[str, str]] = {
        # key: (content, default_extension)
    }

    if fmt in ("text", "all"):
        reports["text"] = (generate_text_report(results, comparison), ".txt")
    if fmt in ("json", "all"):
        reports["json"] = (generate_json_report(results, comparison), ".json")
    if fmt in ("markdown", "all"):
        reports["markdown"] = (generate_markdown_report(results, comparison), ".md")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for fmt_key, (content, ext) in reports.items():
        # Print to console
        if not args.quiet and fmt != "all":
            print("\n" + content)

        # Determine output path
        if args.output and fmt != "all":
            save_report(content, args.output)
        elif args.output_dir or fmt == "all":
            out_dir  = args.output_dir or "./json_reports"
            out_file = os.path.join(out_dir, f"schema_report_{timestamp}{ext}")
            save_report(content, out_file)

    print("\n✅ Done!\n")


if __name__ == "__main__":
    main()
