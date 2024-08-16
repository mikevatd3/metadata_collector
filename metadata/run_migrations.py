import os
from pathlib import Path
import logging
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
import tomli

from connection import db_engine
from app_logger import setup_logging


with open("config.toml", "rb") as f:
    app_config = tomli.load(f)

logger = logging.getLogger(app_config["app"]["name"])


def main():
    setup_logging()
    migrations_dir = Path.cwd() / app_config["app"]["name"] / "migrations"
    available_migrations = sorted(os.listdir(migrations_dir))

    stmt = text("""
    SELECT *
    FROM operations
    ORDER BY ran_at DESC
    LIMIT 1;
    """)
    try:
        try:
            with db_engine.connect() as db:
                result = db.execute(stmt)
                row = result.fetchone()

                last_ran = row.file
                to_run = available_migrations[available_migrations.index(last_ran) + 1:]

                if not to_run:
                    logger.warning("Database is up to date!")

        except ProgrammingError:
            with db_engine.connect() as db:
                initial_file = "0000_DB_create_ops.sql"
                start_script = text((migrations_dir / initial_file).read_text())
                db.execute(start_script)
                insert = text(
                    """
                    INSERT INTO operations VALUES (default, :filename, default);
                    """
                )
                db.execute(insert, {"filename": initial_file})
                db.commit()
                logger.debug("Database Initialized.")

                result = db.execute(stmt)
                row = result.fetchone()
                last_ran = row.file
                to_run = available_migrations[available_migrations.index(last_ran) + 1:]

        with db_engine.connect() as db:
            logger.debug(f"Running migrations: {', '.join(to_run)}")
            for migration in to_run:
                script = text((migrations_dir / migration).read_text())
                db.execute(script)
                stmt = text(
                    """
                    INSERT INTO operations VALUES (default, :filename, default);
                    """
                )
                db.execute(stmt, {"filename": migration})
                db.commit()

    except FileNotFoundError:
        logger.error("Database initial migration failed. Provide first migration.")


if __name__ == "__main__":
    main()
