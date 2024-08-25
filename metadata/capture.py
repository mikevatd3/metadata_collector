import logging

import pandas as pd
from dateutil.parser import parse as parse_date, ParserError as DateParseError
from sqlalchemy import insert, MetaData, Table, select
from itertools import groupby

from prompt_toolkit import prompt
from prompt_toolkit.shortcuts import confirm
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.validation import Validator

from .connection import db_engine
from .app_logger import setup_logging


class RegistrationHandler:
    """
    This class handles registering a dataset provided in the init method.

    Check the migrations file '0001_DOCS_data_tables.sql' for a reference
    on field names and data types.
    """

    def __init__(self, filename, file: pd.DataFrame, config):
        self.filename = filename
        self.file = file
        self.db_engine = db_engine
        self.topic = config["app"]["name"]
        self.logger = logging.getLogger(self.topic)

        setup_logging()

        metadata = MetaData()

        self.dataset_table = Table(
            "datasets", metadata, autoload_with=db_engine
        )
        self.variable_table = Table(
            "variables", metadata, autoload_with=db_engine
        )
        self.edition_table = Table(
            "editions", metadata, autoload_with=db_engine
        )
        self.keyword_table = Table(
            "keywords", metadata, autoload_with=db_engine
        )
        self.tags_table = Table("tags", metadata, autoload_with=db_engine)
        self.standards = Table("standards", metadata, autoload_with=db_engine)

        with db_engine.connect() as db:
            self.keyword_completer = WordCompleter(
                self.get_current_keywords(db)
            )
            self.available_datasets = self.get_available_datasets(db)

        self.dataset_completer = WordCompleter(list(self.available_datasets.keys()))

        def validate_date(date: str):
            try:
                parse_date(date)
                return True
            except DateParseError:
                return False

        self.date_validator = Validator.from_callable(
            validate_date,
            error_message="The date must be in YYYY-MM-DD format.",
        )

    def run_complete_workflow(self):
        dataset_name = prompt(
            f"What is the dataset name for {self.filename}? Enter a new\n"
            "if this is the first time documenting this dataset.\n-> ",
            completer=self.dataset_completer,
        )
        with self.db_engine.connect() as db:
            is_new = dataset_name not in self.available_datasets

            if is_new:
                dataset_details, keywords = self.register_dataset(dataset_name)

                ds_insert_stmt = (
                    insert(self.dataset_table)
                    .values(**dataset_details)
                    .returning(self.dataset_table.c["id"])
                )

                result = db.execute(ds_insert_stmt)
                row = result.fetchone()
                dataset_id = row.id  # Does this really need a try except?

                # TODO lookup and add keywords to tags table
                kw_lookup_stmt = select(self.keyword_table).where(
                    self.keyword_table.c.content.in_(keywords)
                )

                kw_result = db.execute(kw_lookup_stmt)
                previous_kws = {row.content: row.id for row in kw_result}
                new_kws = [kw for kw in keywords if kw not in previous_kws]

                new_kw_insert_stmt = insert(self.keyword_table).returning(
                    self.keyword_table.c.id, self.keyword_table.c.content
                )

                new_kw_result = db.execute(
                    new_kw_insert_stmt,
                    [{"content": keyword} for keyword in new_kws],
                )

                kw_ids = {
                    **previous_kws,
                    **{row.content: row.id for row in new_kw_result.fetchall()},
                }

                self.logger.info(kw_ids)

                db.execute(
                    insert(self.tags_table),
                    [
                        {"dataset_id": dataset_id, "kw_id": keyword_id}
                        for keyword_id in kw_ids.values()
                    ],
                )

                variable_details = self.register_variables(dataset_id)

                for variable, standard in variable_details:
                    v_insert_stmt = insert(self.variable_table).values(
                        **variable
                    )
                    db.execute(v_insert_stmt)

                    # TODO Lookup and append the standards

            else:
                dataset_id, columns = self.available_datasets[dataset_name]

                assert all(col in self.file.columns for col in columns) and all(
                    col in columns for col in self.file.columns
                ), (
                    "The column names of the new dataset don't match the current "
                    "columns. In the ETL script, rename these columns before "
                    "calling the 'document' function."
                )

            edition_details = self.register_edition(dataset_id)

            e_insert_stmt = insert(self.edition_table).values(**edition_details)
            db.execute(e_insert_stmt)

            db.commit()

    def register_dataset(self, dataset_name):
        """
        This is the table-level registration step. This passes a dict
        out to the calling function to actually do the insert this is
        to preserve the rollback option.
        """

        description = prompt(
            "Provide a short description of the table: ",
        )
        unit_of_analysis = prompt(
            "What is the unit of analysis of this table? ",
        )
        universe = prompt(
            "How would you describe the universe of this table? ",
        )
        owner = prompt(
            "Who 'owns' this dataset? ",
        )
        collector = prompt(
            "Who is responsible for collecting this dataset? ",
        )
        collection_method = prompt(
            "Describe the method of collection for this dataset: "
        )
        collection_reason = prompt(
            "Why was this dataset collected (as stated by the collector)? ",
        )
        source_url = prompt(
            "Source url: ",
        )
        notes = prompt(
            "Additional notes: ",
        )
        use_conditions = prompt(
            "Are there any restrictions on sharing this dataset? ",
        )

        # Keywords

        def validate_keywords(value):
            return len(value) > 3

        keyword_validator = Validator.from_callable(
            validate_keywords,
            error_message="Keywords must be longer than three characters.",
        )

        keywords = []
        add_another = True

        while add_another:
            keyword = prompt(
                "Add a keyword: ",
                validator=keyword_validator,
                completer=self.keyword_completer,
            )

            keywords.append(keyword)
            print("Current keywords:", ",".join(keywords))
            add_another = confirm("Add another keyword?")

        def validate_cadence(cadence):
            """
            FUTURE: This should depend on autotime so we can easily add
            """
            return cadence in {
                "month",
                "quarter",
                "year",
                "none",
            }

        cadence_validator = Validator.from_callable(
            validate_cadence,
            error_message="The cadence must be one of 'month', 'quarter', 'year', or 'none'.",
        )

        cadence = prompt(
            "What cadence is the dataset reported at or valid for? (month, quarter, year) ",
            validator=cadence_validator,
        )

        return {
            "table_name": dataset_name,
            "description": description,
            "unit_of_analysis": unit_of_analysis,
            "universe": universe,
            "owner": owner,
            "collector": collector,
            "collection_method": collection_method,
            "collection_reason": collection_reason,
            "source_url": source_url,
            "notes": notes,
            "use_conditions": use_conditions,
            "cadence": cadence,
            "topic": self.topic,
        }, keywords

    def register_variables(self, dataset_id):
        """
        Each variable needs to go through a new workflow.
        """

        datatype_completer = WordCompleter(["numeric", "string", "timestamp"])

        result = []
        for variable_name in self.file.columns:
            print(f"Variable name: {variable_name} ")
            print(f"Example rows:\n{self.file[variable_name].head()}")

            description = prompt(
                "Provide a short description about what this variable is reporting: ",
            )
            data_type = prompt(
                "What is data type of this variable? ",
                completer=datatype_completer,
            )

            parent_variable_validator = Validator.from_callable(
                lambda x: ((x in self.file.columns) & (x != variable_name)),
                f"The parent variable must be a valid variable name, and cannot be itself.",
            )
            parent_variable_completer = WordCompleter(
                [item for item in self.file.columns if item != variable_name]
            )

            has_parent = confirm(
                "Does this variable have a parent variable? ",
            )
            if has_parent:
                parent_variable = prompt(
                    completer=parent_variable_completer,
                    validator=parent_variable_validator,
                )
            else:
                parent_variable = None

            suppression_validator = Validator.from_callable(
                lambda x: x.isnumeric(),
                "The suppression threshold must be a numeric value.",
            )

            is_suppressed = confirm(
                "Is there a level that this data should be suppressed? ",
            )
            if is_suppressed:
                suppression_threshold = prompt(
                    "What is the minimum value that our tools should display? ",
                    validator=suppression_validator,
                )
            else:
                suppression_threshold = None

            is_standard = confirm(
                "Does the variable follow any known standard?"
            )
            if is_standard:
                standard = prompt("Which standard does it follow? ")
            else:
                standard = None

            result.append(
                (
                    {
                        "variable_name": variable_name,
                        "description": description,
                        "dataset_id": dataset_id,
                        "data_type": data_type,
                        "parent_variable": parent_variable,
                        "suppression_threshold": suppression_threshold,
                    },
                    standard,
                )
            )

        return result

    def register_edition(self, dataset_id):
        """
        Each time the dataset is downloaded, the dataset edition has to
        be documented.
        """

        num_records = len(self.file)
        notes = prompt(
            "Are there any edition-specific notes that you'd like to include? "
        )
        publish_date = prompt(
            "What date was this dataset published? ",
            validator=self.date_validator,
        )
        collection_start = prompt(
            "What is the start date of this dataset? ",
            validator=self.date_validator,
        )
        collection_end = prompt(
            "What is the end date of this dataset? ",
            validator=self.date_validator,
        )
        acquisition_date = prompt(
            "What date was this dataset added to the system? ",
            validator=self.date_validator,
        )

        return {
            "dataset_id": dataset_id,
            "num_records": num_records,
            "notes": notes,
            "publish_date": publish_date,
            "collection_start": collection_start,
            "collection_end": collection_end,
            "acquisition_date": acquisition_date,
        }

    def get_available_datasets(self, db):
        stmt = select(
            self.dataset_table.c["id"],
            self.dataset_table.c["table_name"],
            self.variable_table.c["variable_name"],
        ).select_from(
            self.dataset_table.join(
                self.variable_table,
                (self.dataset_table.c.id == self.variable_table.c.dataset_id),
            )
        )

        self.logger.info(stmt.compile())

        result = db.execute(stmt)

        return {
            dataset[1]: (dataset[0], [row[2] for row in rows])
            for dataset, rows in groupby(
                result.fetchall(), lambda row: (row[0], row[1])
            )
        }

    def get_current_keywords(self, db):
        """
        Also a DB read.
        """

        stmt = select(self.keyword_table.c["content"])
        result = db.execute(stmt)

        return [item[0] for item in result]

    def get_current_standards(self):
        """
        And another.
        """

        return []
