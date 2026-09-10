"""Support for aliasing whole module subtrees that have moved.

Used to keep old import paths working (with a DeprecationWarning) after an
internal reorganization, e.g. ``mosaicolabs.models.query`` -> ``mosaicolabs.query``.
"""

from __future__ import annotations

import importlib
import sys
import warnings
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec
from types import ModuleType


class _AliasLoader(Loader):
    def __init__(self, module: ModuleType) -> None:
        self._module = module

    def create_module(self, spec: ModuleSpec) -> ModuleType:
        return self._module

    def exec_module(self, module: ModuleType) -> None:
        # The real module is already fully initialized; nothing to execute.
        pass


class _MovedPackageFinder(MetaPathFinder):
    """Redirects imports of ``old_root`` (and any submodule of it) to
    ``new_root``, aliasing them to the *same* module objects rather than
    re-executing the module under a new name.
    """

    def __init__(self, old_root: str, new_root: str) -> None:
        self._old_root = old_root
        self._new_root = new_root

    def find_spec(self, fullname, path=None, target=None):
        if fullname != self._old_root and not fullname.startswith(self._old_root + "."):
            return None

        new_name = self._new_root + fullname[len(self._old_root) :]
        warnings.warn(
            f"'{fullname}' is deprecated and will be removed in a future release; "
            f"import from '{new_name}' instead.",
            FutureWarning,
            stacklevel=3,
        )

        module = importlib.import_module(new_name)
        spec = ModuleSpec(
            fullname, _AliasLoader(module), origin=getattr(module, "__file__", None)
        )
        if hasattr(module, "__path__"):
            spec.submodule_search_locations = list(module.__path__)
        return spec


def alias_moved_package(old_root: str, new_root: str) -> None:
    """Make ``import <old_root>[.sub...]`` keep working as an alias for
    ``<new_root>[.sub...]``, emitting a ``DeprecationWarning`` on first use
    of each aliased module name."""
    sys.meta_path.insert(0, _MovedPackageFinder(old_root, new_root))
