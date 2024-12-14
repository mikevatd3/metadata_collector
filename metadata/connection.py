from pathlib import Path
from sqlalchemy import create_engine
import tomli

# Walk out the the module base path
config_path = Path.cwd() / "config.toml"

with open(config_path, "rb") as f:
    app_config = tomli.load(f)


db_engine = create_engine(
    f"postgresql+psycopg2://{app_config['db']['user']}:{app_config['db']['password']}"
    f"@{app_config['db']['host']}:{app_config['db']['port']}/{app_config['db']['name']}",
    connect_args={'options': '-csearch_path={}'.format(app_config["db"]["metadata_schema"])}
)
