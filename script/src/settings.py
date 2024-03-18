from pathlib import Path
from loguru import logger

class Settings():
    basedir = Path.cwd()
    rawdir = Path("raw_data")
    processeddir = Path("..\data\raw")
    logdir = basedir / "log"


settings = Settings()
logger.add("logfile.log")