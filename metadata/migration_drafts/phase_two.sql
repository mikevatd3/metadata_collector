CREATE TABLE IF NOT EXISTS projects
(
	id                  SERIAL PRIMARY KEY,
	project_name        VARCHAR(256),
	project_description TEXT,
	start_date          TIMESTAMP
);


CREATE TABLE IF NOT EXISTS usages
(
	source_id   INTEGER,
	project_id  INTEGER,
	description TEXT,
	PRIMARY KEY (source_id, project_id),
	FOREIGN KEY (source_id) REFERENCES sources (id),
	FOREIGN KEY (project_id) REFERENCES projects (id)
);


