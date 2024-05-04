import logging
import logging.handlers
from pathlib import Path

LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "error": logging.ERROR,
    "crit": logging.CRITICAL,
}

logs_dir = Path("./logs")
if not logs_dir.exists():
    logs_dir.mkdir()


class MyLogger:
    def __init__(self, name, file_level="info", stream=False, stream_level="warn"):
        self.filename = logs_dir.joinpath("".join((name, ".log")))
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        # rotate_handler = logging.handlers.RotatingFileHandler(
        #     self.filename, maxBytes=2048, backupCount=3
        # )
        # self.logger.addHandler(rotate_handler)

        formatter_separator = " | "
        formatter_elements = (
            "%(asctime)s",
            "%(levelname)s",
            "%(name)s",
            "%(message)s",
        )
        formatter = logging.Formatter(str.join(formatter_separator, formatter_elements))

        file_handler = logging.FileHandler(self.filename, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(LEVELS[file_level])
        self.logger.addHandler(file_handler)

        if stream:
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(LEVELS[stream_level])
            stream_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

    def crit(self, msg):
        self.logger.critical(msg)
