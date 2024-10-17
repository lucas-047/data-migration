import re


def parse_sql_file(sql_content):
    """Parse an SQL file to extract table definitions, primary keys, and foreign keys.

    Args:
        sql_content (str): sql file content

    Returns:
        list: A list of dictionaries containing table information.
    """

    # Compile regex patterns once
    table_pattern = re.compile(
        r'CREATE TABLE IF NOT EXISTS `(\w+)` \((.*?)\) ENGINE=', re.DOTALL)
    column_pattern = re.compile(
        r"`(\w+)` (\w+\(?\d*\)?) (NOT NULL|DEFAULT NULL)")
    primary_key_pattern = re.compile(r"PRIMARY KEY \(`(\w+)`\)")
    unique_key_pattern = re.compile(r"UNIQUE KEY `([^`]+)` \(([^)]+)\)")
    alter_table_pattern = r'ALTER TABLE `([^`]+)`\s+(.*?);'
    constraint_pattern = re.compile(
        r'ADD CONSTRAINT `([^`]+)` FOREIGN KEY \(`([^`]+)`\) REFERENCES `([^`]+)` \(`([^`]+)`\)',
        re.IGNORECASE
    )

    tables = []
    table_map = {}

    # Process the SQL content in a single pass for CREATE TABLE
    for create_match in table_pattern.finditer(sql_content):
        table_name = create_match.group(1)
        columns_definition = create_match.group(2)

        columns = []
        for column_match in column_pattern.finditer(columns_definition):
            column_name = column_match.group(1)
            data_type = column_match.group(2)
            not_null = column_match.group(3) == "NOT NULL"
            columns.append({
                "column_name": column_name,
                "data_type": data_type,
                "not_null": not_null
            })

        if primary_key_pattern.search(columns_definition):
            primary_key = primary_key_pattern.search(
                columns_definition).group(1)

        unique_key_info = {}

        for unique_key_match in unique_key_pattern.finditer(columns_definition):
            unique_key_info = {
                "four_me": unique_key_match.group(1),
                "columns": [unique_key_match.group(2)]
            }

        table_entry = {
            "table_name": table_name,
            "columns": columns,
            "primary_key": primary_key,
            "foreign_keys": [],
            "unique_key": unique_key_info
        }
        tables.append(table_entry)
        table_map[table_name] = table_entry

    # Process ALTER TABLE statements for primary and foreign keys
    for alter_match in re.finditer(alter_table_pattern, sql_content, re.DOTALL):
        table_name = alter_match.group(1)
        alter_body = alter_match.group(2)

        # Check for foreign key constraints
        for constraint in constraint_pattern.finditer(alter_body):
            foreign_key_info = {
                "constraint_name": constraint.group(1),
                "foreign_key_column": constraint.group(2),
                "referenced_table": constraint.group(3),
                "referenced_column": constraint.group(4)
            }
            if table_name in table_map:
                table_map[table_name]['foreign_keys'].append(foreign_key_info)
                print("parsed table :", table_name)

    return tables


def parse_mssql_file(sql_content):

    table_pattern = re.compile(
        r"CREATE TABLE\s+\[([^\]]+)\]\.\[([^\]]+)\]\((.*?)\s*GO", re.DOTALL)
    column_with_size_pattern = re.compile(
        r"\[(\w+)\]\s*\[(\w+)\]\s*\((\w+)(?:\s*,\s*(max|\d+))?\)\s*(NULL|NOT NULL)", re.IGNORECASE)
    column_identity_pattern = re.compile(
        r"\[(\w+)\] \[(\w+)\] IDENTITY\s*\(\d+,\d+\) (NOT NULL)", re.IGNORECASE)
    column_without_size_pattern = re.compile(
        r"\[(\w+)\] \[(\w+)\]\s*(NULL|NOT NULL)", re.IGNORECASE)
    primary_key_pattern = re.compile(
        r"CONSTRAINT \[(\w+)\] PRIMARY KEY", re.IGNORECASE)

    tables = []

    for create_match in table_pattern.finditer(sql_content):
        database_name = create_match.group(1)
        table_name = create_match.group(2)
        columns_definition = create_match.group(3)

        columns = []
        for column_match in column_identity_pattern.finditer(columns_definition):
            column_name = column_match.group(1)
            data_type = column_match.group(2)
            nullability = column_match.group(3) == 'NULL'
            columns.append({
                "column_name": column_name,
                "data_type": data_type,
                "not_null": not nullability
            })

        for column_match in column_with_size_pattern.finditer(columns_definition):
            column_name = column_match.group(1)
            data_type = column_match.group(2)
            nullability = column_match.group(5) == 'NULL'
            columns.append({
                "column_name": column_name,
                "data_type": data_type,
                "not_null": not nullability
            })

        for column_match in column_without_size_pattern.finditer(columns_definition):
            column_name = column_match.group(1)
            data_type = column_match.group(2)
            nullability = column_match.group(3) == 'NULL'
            columns.append({
                "column_name": column_name,
                "data_type": data_type,
                "not_null": not nullability
            })

        primary_key_match = primary_key_pattern.search(columns_definition)
        if primary_key_match != None:
            primary_key = primary_key_match.group(1)
        else:
            primary_key = None

        table_entry = {
            "database_name": database_name,
            "table_name": table_name,
            "columns": columns,
            "primary_key": primary_key,
        }

        tables.append(table_entry)
    return tables
