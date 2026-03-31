# Databricks notebook source
# DBTITLE 1,Config Loader
import yaml
import os


def load_config() -> dict:
    """Load config.yaml from the repo root directory."""
    config_paths = [
        os.path.join(os.getcwd(), "config.yaml"),
        os.path.join(os.getcwd(), "..", "config.yaml"),
        "../config.yaml",
        "config.yaml",
    ]
    # Also try __file__-relative if available
    if "__file__" in dir():
        config_paths.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config.yaml"))
    for path in config_paths:
        try:
            resolved = os.path.realpath(path)
            if os.path.exists(resolved):
                with open(resolved) as f:
                    return yaml.safe_load(f)
        except Exception:
            continue
    raise FileNotFoundError(
        "config.yaml not found. Searched: " + ", ".join(config_paths)
    )
