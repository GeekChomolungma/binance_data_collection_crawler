"""Binance public-archive crawler (data.binance.vision).

Modules
-------
config   : load / validate ``config/system.yaml``
symbols  : discover active <quote>-quoted symbols via the public exchangeInfo API
vision   : list, build URLs for, download, verify and extract archive files
crawler  : orchestrate a full crawl from the config + symbols file
panel    : stitch the downloaded per-symbol CSVs into one long-format panel CSV
cli      : ``python -m binance_archive`` entry point
"""

__all__ = ["config", "symbols", "vision", "crawler", "panel"]
__version__ = "0.1.0"
