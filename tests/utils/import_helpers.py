"""
Helpers for loading service modules with hyphenated package names.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Iterable, Tuple


INGESTION_ROOT = Path(__file__).resolve().parents[2]


def _create_stub_class(name: str):
    """Create a stub class that raises if instantiated without being mocked."""

    def __init__(self, *args, **kwargs):  # type: ignore[no-redef]
        raise RuntimeError(f"Stub for {name} should be mocked in tests before use")

    return type(name, (), {"__init__": __init__})


STUB_MODULES = {
    "aiokafka": {
        "AIOKafkaProducer": _create_stub_class("AIOKafkaProducer"),
        "AIOKafkaConsumer": _create_stub_class("AIOKafkaConsumer"),
    },
    "aiokafka.errors": {
        "KafkaError": _create_stub_class("KafkaError"),
    },
}


def _ensure_namespace_package(name: str, actual_path: Path) -> ModuleType:
    """Ensure a namespace package exists for the given name."""
    module = sys.modules.get(name)
    if module is None:
        module = ModuleType(name)
        module.__path__ = [str(actual_path)]
        sys.modules[name] = module
    else:
        pkg_paths = getattr(module, "__path__", [])
        if str(actual_path) not in pkg_paths:
            module.__path__ = list(pkg_paths) + [str(actual_path)]

    if "." in name:
        parent_name, attr = name.rsplit(".", 1)
        parent_module = sys.modules.get(parent_name)
        if parent_module is not None and not hasattr(parent_module, attr):
            setattr(parent_module, attr, module)
    return module


def load_module(module_path: str) -> ModuleType:
    """
    Load a module from the services directory, supporting hyphenated package names.

    Args:
        module_path: Dotted module path using on-disk directory names.

    Returns:
        Loaded module object.
    """
    sanitized_name = module_path.replace("-", "_")
    if sanitized_name in sys.modules:
        return sys.modules[sanitized_name]

    parts = module_path.split(".")
    sanitized_parts = sanitized_name.split(".")

    # Ensure parent namespace packages exist
    for idx in range(len(parts) - 1):
        package_name = ".".join(sanitized_parts[: idx + 1])
        actual_package_path = INGESTION_ROOT / Path(*parts[: idx + 1])
        _ensure_namespace_package(package_name, actual_package_path)

    module_rel_path = Path(*parts).with_suffix(".py")
    module_file = INGESTION_ROOT / module_rel_path

    if not module_file.exists():
        raise ImportError(f"Module file not found for {module_path}: {module_file}")

    # Provide stub modules for optional dependencies that aren't installed in test environments
    for stub_name, attributes in STUB_MODULES.items():
        if stub_name not in sys.modules:
            stub_module = ModuleType(stub_name)
            stub_module.__dict__.update(attributes)
            sys.modules[stub_name] = stub_module

    spec = importlib.util.spec_from_file_location(sanitized_name, module_file)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load spec for {module_path}")

    module = importlib.util.module_from_spec(spec)
    module.__package__ = ".".join(sanitized_parts[:-1])
    sys.modules[sanitized_name] = module

    spec.loader.exec_module(module)  # type: ignore[assignment]

    # Attach module to parent package for attribute access
    if sanitized_parts[:-1]:
        parent_name = ".".join(sanitized_parts[:-1])
        parent_module = sys.modules.get(parent_name)
        if parent_module is not None:
            setattr(parent_module, sanitized_parts[-1], module)

    return module


def import_from_services(module_path: str, names: Iterable[str]) -> Tuple:
    """
    Import specific symbols from a service module with hyphenated directories.

    Args:
        module_path: Dotted module path (matching on-disk structure).
        names: Iterable of attribute names to extract.

    Returns:
        Tuple of requested attributes in order.
    """
    module = load_module(module_path)
    return tuple(getattr(module, name) for name in names)
