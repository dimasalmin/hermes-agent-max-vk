"""Make plugin tests reproducible with an externally installed Hermes tree.

Hermes owns a top-level ``plugins`` package.  The repository provides
additional modules below that namespace, so tests must extend the installed
package path instead of shadowing or modifying Hermes itself.
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    import plugins as _hermes_plugins
except ImportError:
    _hermes_plugins = None
else:
    plugin_root = str(REPO_ROOT / "plugins")
    namespace_path = getattr(_hermes_plugins, "__path__", None)
    if namespace_path is not None and plugin_root not in namespace_path:
        namespace_path.append(plugin_root)
