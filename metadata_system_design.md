- Table Metadata
  - id - pk
  - table_name
  - table_description
  - data_collector
  - data_owner
  - collection_method
  - unit of analysis
  - universe
  - source_url
  - notes
  - usage_conditions
  - keywords - m to m

- Variable Metadata
  - id
  - variable_name
  - data_type
  - table_id
  - parent_variable
  - suppression_threshold

- Variable Standards
  - id
  - description
  - maintainer

- Autotime
  - table_id - pk - 1 to 1
  - frame_type
  - resolution
  - available_from
  - available_to


- Pipeline (Editions)
  - id - pk
  - table_id
  - publish_date
  - acquisition_date
  - start_date
  - end_date
