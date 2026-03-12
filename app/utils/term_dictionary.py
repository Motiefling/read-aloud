"""
Term dictionary loading and application.

Handles loading YAML term dictionaries (global and per-novel)
and applying them to translated text for consistency.
"""

from pathlib import Path

import yaml

from app.config import BASE_DIR


DICTIONARIES_DIR = BASE_DIR / "data" / "dictionaries"


def load_dictionary(novel_id: str | None = None) -> dict:
    """
    Load a term dictionary. If novel_id is provided, loads the novel-specific
    dictionary merged with the global dictionary. Novel-specific entries
    take precedence over global entries.
    """
    # Load global dictionary
    global_path = DICTIONARIES_DIR / "global.yaml"
    global_dict = _load_yaml(global_path) if global_path.exists() else {}

    if novel_id is None:
        return global_dict

    # Load novel-specific dictionary and merge
    novel_path = DICTIONARIES_DIR / f"{novel_id}.yaml"
    if novel_path.exists():
        novel_dict = _load_yaml(novel_path)
        return _merge_dicts(global_dict, novel_dict)

    return global_dict


def save_dictionary(novel_id: str, entries: dict) -> None:
    """Save a term dictionary for a specific novel."""
    DICTIONARIES_DIR.mkdir(parents=True, exist_ok=True)
    path = DICTIONARIES_DIR / f"{novel_id}.yaml"
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(entries, f, allow_unicode=True, default_flow_style=False)


def _load_yaml(path: Path) -> dict:
    """Load a YAML file and return its contents as a dict."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _merge_dicts(base: dict, override: dict) -> dict:
    """Deep-merge two dictionaries. Override values take precedence."""
    merged = base.copy()
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged
