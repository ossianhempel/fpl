import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging() -> None:
    # create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # clear existing handlers to prevent duplicates
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    # handlers to determine WHERE logs go

    # console handler - prints to terminal/console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(
        logging.INFO
    )  # only log messages with level INFO or higher to the terminal to avoid clutter
    console_format = logging.Formatter("%(levelname)s - %(message)s")
    console_handler.setFormatter(console_format)

    # file handler - writes to file
    # rotating handler to manage log files that grow over time
    # automatically creates new log files and archive old ones based on the given thresholds
    file_handler = RotatingFileHandler(
        filename=log_dir / "app.log",
        maxBytes=1024 * 1024,  # 1mb
        backupCount=5,
    )
    file_handler.setLevel(
        logging.DEBUG
    )  # logs messages of all levels as DEBUG is the lowest level
    file_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_format)

    # add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
