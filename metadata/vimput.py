import tempfile, os
from subprocess import call
import logging
import tomli

from app_logger import setup_logging


def gather_text_with_editor(initial_message="Enter your input below"):
    EDITOR = os.environ.get("EDITOR", "nvim")
    message = f"""<!--{initial_message}-->""".encode()

    with tempfile.NamedTemporaryFile(suffix=".tmp") as tf:
        tf.write(message)
        tf.flush()
        call([EDITOR, tf.name])

        tf.seek(0)
        return tf.read().decode().strip()


if __name__ == "__main__":
    with open("config.toml", "rb") as f:
        config = tomli.load(f)

    logger = logging.getLogger(config["app"]["name"])
    setup_logging()

    editied_message = gather_text_with_editor()
    logger.info(editied_message)
