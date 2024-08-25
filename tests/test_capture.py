import logging
import pytest
import pandas as pd
import tomli

from metadata.capture import RegistrationHandler
from metadata.app_logger import setup_logging




@pytest.fixture()
def config():
    with open("config.toml", "rb") as f:
        config = tomli.load(f)

    return config


@pytest.fixture()
def logger(config):
    return logging.getLogger(config["app"]["name"])


@pytest.fixture()
def dataset():
    return pd.DataFrame(
        {
            "apple_name": ["mac", "ipad", "ipod", "lisa"],
            "active": [True, True, False, False],
        }
    )


def test_setup_handler(dataset, config, logger):
    handler = RegistrationHandler(
        "apples",
        dataset,
        config
    )

    logger.info(handler.available_datasets)

    assert True
