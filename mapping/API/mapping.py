import re
import os
import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from django.http import JsonResponse, FileResponse  # Add FileResponse here
def extract_columns(sql_content):
    columns_with_types = {}
    primary_keys = set()
    foreign_keys = {}
    table_names = []  # List to store table names

    # Remove comments from SQL content
    sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)  # Remove block comments
    sql_content = re.sub(r'--.*?\n', '', sql_content)  # Remove single-line comments

    # Regex pattern to capture CREATE TABLE and its columns
    create_table_pattern = r'CREATE TABLE\s+`?(\w+)`?\s*\((.*?)\)\s*ENGINE='

    # Find all CREATE TABLE statements
    matches = re.findall(create_table_pattern, sql_content, re.IGNORECASE | re.DOTALL)

    for table_name, columns_definition in matches:
        table_names.append(table_name)  # Add table name to the list

        # Split the column definitions by commas
        column_defs = re.split(r',\s*(?=(?:[^()]*\([^()]*\))*[^()]*$)', columns_definition.strip())

        for column_def in column_defs:
            # Match the column name and type
            match = re.match(r'`?(\w+)`?\s+([^\s,()]+(?:\s*\(\d+(?:,\d+)?\))?)(.*)', column_def.strip())
            if match:
                column_name = match.group(1)
                data_type = match.group(2).strip()
                columns_with_types[column_name] = data_type

            # Check for primary key
            if 'PRIMARY KEY' in column_def:
                primary_key_match = re.search(r'PRIMARY KEY\s*\(`?(\w+)`?\)', column_def)
                if primary_key_match:
                    primary_keys.add(primary_key_match.group(1))

            # Check for foreign key
            if 'FOREIGN KEY' in column_def:
                foreign_key_match = re.search(
                    r'FOREIGN KEY\s*\(`?(\w+)`?\)\s+REFERENCES\s+`?(\w+)`?\s*\(`?(\w+)`?\)',
                    column_def
                )
                if foreign_key_match:
                    foreign_keys[foreign_key_match.group(1)] = {
                        "references_table": foreign_key_match.group(2),
                        "references_column": foreign_key_match.group(3)
                    }
    
    # Return columns, primary keys, foreign keys, and table names
    return columns_with_types, primary_keys, foreign_keys, table_names
@csrf_exempt
def mapping_data(request):
    if request.method != 'POST':
        return JsonResponse({"error": "POST request required."}, status=400)

    uploaded_files = request.session.get('uploaded_files', {})

    # Retrieve file names from session
    source_file_name = uploaded_files.get('source_file')
    target_file_name = uploaded_files.get('target_file')

    if not source_file_name or not target_file_name:
        return JsonResponse({"error": "Files not found in session. Please upload the files first."}, status=400)

    # Define the upload directory
    upload_dir = os.path.join(settings.MEDIA_ROOT, 'uploads')

    # Get the file paths from the session
    source_file_path = os.path.join(upload_dir, source_file_name)
    target_file_path = os.path.join(upload_dir, target_file_name)

    if not os.path.exists(source_file_path) or not os.path.exists(target_file_path):
        return JsonResponse({"error": "Files do not exist on the server."}, status=400)

    # Read source file content
    with open(source_file_path, 'r',encoding='utf-8') as file:
        source_sql_content = file.read()

    # Read target file content
    with open(target_file_path, 'w',encoding='utf-8') as file:
        target_sql_content = file.read()

    # Extract columns, primary keys, and foreign keys from source and target
    source_columns, source_primary_keys, source_foreign_keys, source_table_names = extract_columns(source_sql_content)
    target_columns, target_primary_keys, target_foreign_keys, target_table_names = extract_columns(target_sql_content)

    # Assuming mappings are sent in request body
    data = json.loads(request.body)
    mappings_list = data.get('mappings', [])

    # Validate mappings and collect data to insert
    all_insert_statements = []

    for mapping in mappings_list:
        source_table = mapping.get('source_table')
        source_columns = mapping.get('source_columns', [])
        target_table = mapping.get('target_table')
        target_columns = mapping.get('target_columns', [])

        # Check if source table exists
        if source_table not in source_table_names:
            return JsonResponse({"error": f"Source table '{source_table}' not found."}, status=400)

        # Check if target table exists
        if target_table not in target_table_names:
            return JsonResponse({"error": f"Target table '{target_table}' not found."}, status=400)

        # Validate source columns
        for source_col in source_columns:
            if source_col not in source_columns:
                return JsonResponse({"error": f"Source column '{source_col}' not found in table '{source_table}'."}, status=400)

        # Validate target columns
        for target_col in target_columns:
            if target_col not in target_columns:
                return JsonResponse({"error": f"Target column '{target_col}' not found in table '{target_table}'."}, status=400)

        # Extract data from the source file for this mapping
        source_data = extract_data_from_source(source_sql_content, source_columns)

        # Generate new INSERT statements for the target file based on the mapping
        target_insert_statements = generate_insert_statements(
            target_table,  # Use the target table from the mapping
            target_columns,
            source_data,
            dict(zip(source_columns, target_columns))  # Map source to target columns
        )

        all_insert_statements.extend(target_insert_statements)  # Collect all statements

    # Write all new INSERT statements to the target file
    with open(target_file_path, 'a') as target_file:
        target_file.write("\n".join(all_insert_statements) + "\n")

    return JsonResponse({
        "message": "Data copied successfully",
        "total_insert_statements": len(all_insert_statements),
        "mappings_processed": len(mappings_list)
    })

def extract_data_from_source(sql_content, columns):
    """Extracts data from the source SQL file for the given columns."""
    data = []
    insert_pattern = re.compile(r"INSERT INTO `?\w+`?\s*\((.*?)\)\s*VALUES\s*\((.*?)\);", re.IGNORECASE | re.DOTALL)
    matches = insert_pattern.findall(sql_content)

    for column_list, value_list in matches:
        column_names = [col.strip().strip("`") for col in column_list.split(',')]
        values = [val.strip() for val in value_list.split(',')]

        row_data = {}
        for col, val in zip(column_names, values):
            if col in columns:
                row_data[col] = val
        data.append(row_data)

    return data

def generate_insert_statements(target_table_name, target_columns, source_data, mappings):
    """Generates INSERT statements for the target table based on the source data and mappings."""
    insert_statements = []

    for index, source_row in enumerate(source_data):
        target_cols = []
        values = []
        
        for source_col, target_col in mappings.items():
            source_value = source_row.get(source_col)

            if source_value is not None:  # Ensure we only add non-None values
                values.append(f"'{source_value}'")  # Wrap the value in quotes for SQL
                target_cols.append(target_col)  # Add target column to be inserted

        if values:  # If there are valid values to insert
            columns_part = ", ".join(f"`{col}`" for col in target_cols)
            values_part = ", ".join(values)
            insert_statement = f"INSERT INTO {target_table_name} ({columns_part}) VALUES ({values_part});"
            insert_statements.append(insert_statement)

    return insert_statements
