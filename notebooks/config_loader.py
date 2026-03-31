# Databricks notebook source
# DBTITLE 1,Config Loader
"""
Shared config loader for all notebooks.
Reads config.yaml from the repo root and returns a dict.
Usage in other notebooks:
    %run ./config_loader
    cfg = load_config()
"""

import yaml
import os


def load_config() -> dict:
    """Load config.yaml from the repo root directory."""
    config_paths = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config.yaml"),
        "/Workspace/Repos/config.yaml",
        "../config.yaml",
    ]
    for path in config_paths:
        resolved = os.path.realpath(path)
        if os.path.exists(resolved):
            with open(resolved) as f:
                return yaml.safe_load(f)
    raise FileNotFoundError(
        "config.yaml not found. Searched: " + ", ".join(config_paths)
    )
