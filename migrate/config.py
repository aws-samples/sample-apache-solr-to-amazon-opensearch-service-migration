import logging
import coloredlogs

coloredlogs.install()
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler = logging.FileHandler("../logs.log", "w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)


def get_custom_logger(name):
    logger = logging.getLogger(name)
    logger.addHandler(file_handler)
    return logger

# logger.setLevel(logging.DEBUG)


# File Logging
