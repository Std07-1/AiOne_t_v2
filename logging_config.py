# logging_config.py
import logging
import logging.config
from rich.console import Console
from rich.logging import RichHandler

console = Console()

LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        },
    },
    "handlers": {
        "rich_console": {
            "class": "rich.logging.RichHandler",
            "formatter": "default",
            "level": "DEBUG",
            "console": console,
            "show_path": False,
        },
        "file": {
            "class": "logging.FileHandler",
            "formatter": "default",
            "level": "DEBUG",
            "filename": "system.log",
            "encoding": "utf-8",
            "mode": "a",
        },
    },
    "loggers": {
        "main": {
            "handlers": ["rich_console", "file"],
            "level": "DEBUG",
            "propagate": False,
        },
        "raw_data": {
            "handlers": ["rich_console", "file"],
            "level": "INFO",
            "propagate": False,
        },
        # Додаткові модулі можна налаштувати окремо
    },
}

logging.config.dictConfig(LOG_CONFIG)
