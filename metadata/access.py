from sqlalchemy import insert, MetaData, Table, select

from .connection import db_engine
from itertools import groupby


class MetadataConnection:
    def __init__(self, logger):
        self.logger = logger
        self.db_engine = db_engine

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

    def insert_dataset(self, dataset: dict, db):
        ds_insert_stmt = (
            insert(self.dataset_table)
            .values(**dataset)
            .returning(self.dataset_table.c["id"])
        )

        result = db.execute(ds_insert_stmt)
        row = result.fetchone()
        return row.id  # Does this really need a try except?


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

    def insert_new_keywords(self, kws: list[str], db):
        """
        This creates all keywords supplied as arguments, and
        returns a dictionary mapping those keywords to their ids.
        """
        new_kw_insert_stmt = insert(self.keyword_table).returning(
            self.keyword_table.c.id, self.keyword_table.c.content
        )

        result = db.execute(
            new_kw_insert_stmt,
            [{"content": keyword} for keyword in kws],
        )

        return {row.content: row.id for row in result.fetchall()}

    def get_all_keywords(self, db):
        stmt = select(self.keyword_table.c.id, self.keyword_table.c.content)
        result = db.execute(stmt)

        return {row.content: row.id for row in result}

    def tag_dataset(self, kw_ids: list[int], ds_id: int, db):
        db.execute(
            insert(self.tags_table),
            [
                {"dataset_id": ds_id, "kw_id": kw_id}
                for kw_id in kw_ids
            ],
        )

    def insert_variables(self, variables: list[dict], dataset_id, db):
        for variable, _ in variables: # Ignore the standard for now
            variable["dataset_id"] = dataset_id
            v_insert_stmt = insert(self.variable_table).values(
                **variable
            )
            db.execute(v_insert_stmt)

            # TODO Lookup and append the standards

    def insert_standard(self, standard: dict):
        pass

    def get_current_standards(self, db):
        """
        And another.
        """

        return []

    def insert_edition(self, edition: dict, dataset_id, db):
        edition["dataset_id"] = dataset_id
        e_insert_stmt = insert(self.edition_table).values(**edition)
        db.execute(e_insert_stmt)
