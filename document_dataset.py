from pathlib import Path
import logging
import pandas as pd
import tomli

from metadata.capture import RegistrationHandler
from metadata.app_logger import setup_logging


def main():
    setup_logging()
    with open("config.toml", "rb") as f:
        config = tomli.load(f)

    logger = logging.getLogger(config["app"]["name"])
    filename = "munoz_llcs.csv"
    file = pd.read_csv(Path.cwd() / "tests" / "fixtures" / filename)
    handler = RegistrationHandler(filename, file)
    handler.run_complete_workflow()
    logger.info(f"{filename} successfully logged.")


if __name__ == "__main__":
    main()
