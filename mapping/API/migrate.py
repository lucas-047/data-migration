import re
import os
import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from django.http import JsonResponse, FileResponse  # Add FileResponse here

# from target_file_upload import upload_sql_file
def extract_columns(sql_content):
    columns_with_types = {}
    primary_keys = set()
    foreign_keys = {}
    table_names = []  # List to store table names

    # Remove comments from SQL content
    sql_content = re.sub(r'/\.?\*/', '', sql_content, flags=re.DOTALL)  # Remove block comments
    sql_content = re.sub(r'--.*?\n', '', sql_content)  # Remove single-line comments

    # Regex pattern to capture CREATE TABLE and its columns
    create_table_pattern = r'CREATE TABLE\s+?(\w+)?\s*\((.*?)\)\s*ENGINE='

    # Find all CREATE TABLE statements
    matches = re.findall(create_table_pattern, sql_content, re.IGNORECASE | re.DOTALL)

    for table_name, columns_definition in matches:
        table_names.append(table_name)  # Add table name to the list

        # Split the column definitions by commas, ensuring to handle multi-line cases
        column_defs = re.split(r',\s*(?=(?:[^()]\([^()]\))[^()]$)', columns_definition.strip())

        for column_def in column_defs:
            # Match the column name and type
            match = re.match(r'?(\w+)?\s+([^\s,()]+(?:\s*\(\d+(?:,\d+)?\))?)(.*)', column_def.strip())
            if match:
                column_name = match.group(1)
                data_type = match.group(2).strip()  # Get the data type and remove extra spaces
                columns_with_types[column_name] = data_type

            # Check for primary key
            if 'PRIMARY KEY' in column_def:
                primary_key_match = re.search(r'PRIMARY KEY\s*\(?(\w+)?\)', column_def)
                if primary_key_match:
                    primary_keys.add(primary_key_match.group(1))

            # Check for foreign key
            if 'FOREIGN KEY' in column_def:
                foreign_key_match = re.search(
                    r'FOREIGN KEY\s*\(?(\w+)?\)\s+REFERENCES\s+?(\w+)?\s*\(?(\w+)?\)',
                    column_def
                )
                if foreign_key_match:
                    foreign_keys[foreign_key_match.group(1)] = {
                        "references_table": foreign_key_match.group(2),
                        "references_column": foreign_key_match.group(3)
                    }
    table_name = table_names[0] if table_names else None

    return columns_with_types, primary_keys, foreign_keys, table_name  # Return table names

# def extract_columns(sql_content):
#         columns_with_types = {}
#         primary_keys = set()
#         foreign_keys = {}

#         # Remove comments from SQL content
#         sql_content = re.sub(r'/\.?\*/', '', sql_content, flags=re.DOTALL)  # Remove block comments
#         sql_content = re.sub(r'--.*?\n', '', sql_content)  # Remove single-line comments

#         # Regex pattern to capture CREATE TABLE and its columns
#         create_table_pattern = r'CREATE TABLE\s+?\w+?\s*\((.*?)\)\s*ENGINE='

#         # Find all CREATE TABLE statements
#         matches = re.findall(create_table_pattern, sql_content, re.IGNORECASE | re.DOTALL)

#         for columns_definition in matches:
#             # Split the column definitions by commas, ensuring to handle multi-line cases
#             column_defs = re.split(r',\s*(?=(?:[^()]\([^()]\))[^()]$)', columns_definition.strip())

#             for column_def in column_defs:
#                 # Match the column name and type
#                 match = re.match(r'?(\w+)?\s+([^\s,()]+(?:\s*\(\d+(?:,\d+)?\))?)(.*)', column_def.strip())
#                 if match:
#                     column_name = match.group(1)
#                     data_type = match.group(2).strip()  # Get the data type and remove extra spaces
#                     columns_with_types[column_name] = data_type

#                 # Check for primary key
#                 if 'PRIMARY KEY' in column_def:
#                     primary_key_match = re.search(r'PRIMARY KEY\s*\(?(\w+)?\)', column_def)
#                     if primary_key_match:
#                         primary_keys.add(primary_key_match.group(1))

#                 # Check for foreign key
#                 if 'FOREIGN KEY' in column_def:
#                     foreign_key_match = re.search(
#                         r'FOREIGN KEY\s*\(?(\w+)?\)\s+REFERENCES\s+?(\w+)?\s*\(?(\w+)?\)',
#                         column_def
#                     )
#                     if foreign_key_match:
#                         foreign_keys[foreign_key_match.group(1)] = {
#                             "references_table": foreign_key_match.group(2),
#                             "references_column": foreign_key_match.group(3)
#                         }

#         return columns_with_types, primary_keys, foreign_keys

# def extract_data_from_source(sql_content, columns):
#     """Extracts data from the source SQL file for the given columns."""
#     data = []
#     insert_pattern = re.compile(r"INSERT INTO ?\w+?\s*\((.?)\)\s*VALUES\s\((.*?)\);", re.IGNORECASE | re.DOTALL)
#     matches = insert_pattern.findall(sql_content)

#     for column_list, value_list in matches:
#         column_names = [col.strip().strip("`") for col in column_list.split(',')]
#         values = [val.strip() for val in value_list.split(',')]

#         row_data = {}
#         for col, val in zip(column_names, values):
#             if col in columns:
#                 row_data[col] = val
#         data.append(row_data)

#     return data

# def generate_insert_statements(target_columns, source_data, mappings):
#     """Generates INSERT statements for the target file using the mapped data."""
#     insert_statements = []

#     for row_data in source_data:
#         target_values = []
#         for target_col in target_columns:
#             source_col = get_source_column_for_target(target_col, mappings)
#             if source_col and source_col in row_data:
#                 target_values.append(row_data[source_col])
#             else:
#                 target_values.append('NULL')  # Handle missing values

#         # Construct the INSERT statement
#         insert_statement = f"INSERT INTO target_table ({', '.join(target_columns)}) VALUES ({', '.join(target_values)});"
#         insert_statements.append(insert_statement)

#     return insert_statements

# def get_source_column_for_target(target_col, mappings):
#     """Finds the source column corresponding to a target column."""
#     for source_col, mapped_target_col in mappings.items():
#         if mapped_target_col == target_col:
#             return source_col
#     return None

# @csrf_exempt
# def mapping_data(request):
#     if request.method != 'POST':
#         return JsonResponse({"error": "POST request required."}, status=400)

#     # Retrieve file names from session
#     source_file_name = request.session.get('source_file')
#     target_file_name = request.session.get('target_file')

#     if not source_file_name or not target_file_name:
#         return JsonResponse({"error": "Files not found in session. Please upload the files first."}, status=400)

#     # Define the upload directory
#     upload_dir = os.path.join(settings.MEDIA_ROOT, 'uploads')

#     # Get the file paths from the session
#     source_file_path = os.path.join(upload_dir, source_file_name)
#     target_file_path = os.path.join(upload_dir, target_file_name)

#     if not os.path.exists(source_file_path) or not os.path.exists(target_file_path):
#         return JsonResponse({"error": "Files do not exist on the server."}, status=400)

#     # Read source file content
#     with open(source_file_path, 'r') as file:
#         source_sql_content = file.read()

#     # Read target file content
#     with open(target_file_path, 'r') as file:
#         target_sql_content = file.read()

#     # Extract columns, primary keys, and foreign keys from source and target
#     source_columns, source_primary_keys, source_foreign_keys = extract_columns(source_sql_content)
#     target_columns, target_primary_keys, target_foreign_keys = extract_columns(target_sql_content)
#     print(source_columns)
#     print(target_columns)

#     # Assuming mappings are sent in request body
#     data = json.loads(request.body)
#     mappings = data.get('mappings', {})

#     # Validate mappings
#     for source_col, target_col in mappings.items():
#         if source_col not in source_columns:
#             return JsonResponse({"error": f"Source column '{source_col}' not found."}, status=400)
#         if target_col not in target_columns:
#             return JsonResponse({"error": f"Target column '{target_col}' not found."}, status=400)

#     # Extract data from the source file
#     source_data = extract_data_from_source(source_sql_content, list(mappings.keys()))

#     # Generate new INSERT statements for the target file based on the mapping
#     target_insert_statements = generate_insert_statements(target_columns, source_data, mappings)
#     output_file_path = os.path.join(upload_dir, 'output.sql')
#     with open(output_file_path, 'w') as output_file:
#         output_file.write("\n".join(target_insert_statements))

#     # Return a response with the file to download
#     response = FileResponse(open(output_file_path, 'rb'), as_attachment=True, filename='output.sql')
#     return response

#     # Optionally, write the new target data into the target file
#     # with open(target_file_path, 'a') as target_file:
#     #     target_file.write("\n".join(target_insert_statements))
#     # return JsonResponse({
#     #     "message": "Data copied successfully",
#     #     "source_columns": source_columns,
#     #     "target_columns": target_columns,
#     #     "source_primary_keys": list(source_primary_keys),
#     #     "target_primary_keys": list(target_primary_keys),
#     #     "source_foreign_keys": source_foreign_keys,
#     #     "target_foreign_keys": target_foreign_keys
#     # })
def generate_insert_statements(target_table_name, target_columns, source_data, mappings):
    """Generates INSERT statements for the target table based on the source data and mappings."""
    insert_statements = []

    # Debugging: Print the inputs for validation
    print("Target Table Name: ", target_table_name)
    print("Target Columns: ", target_columns)
    print("Mappings: ", mappings)
    print("Source Data: ", source_data)

    # Loop over each source row (source data)
    for index, source_row in enumerate(source_data):
        print(f"\nProcessing Source Row {index + 1}: {source_row}")

        # Prepare lists for SQL column names and values
        target_cols = []
        values = []
        
        # Iterate over the mappings to process only mapped columns
        for source_col, target_col in mappings.items():
            # Extract the value from the source data
            source_value = source_row.get(source_col)

            # Debugging: Check what value is being fetched
            print(f"  Mapping: Source Column: {source_col} -> Target Column: {target_col}, Value: {source_value}")

            if source_value is not None:  # Ensure we only add non-None values
                values.append(f"'{source_value}'")  # Wrap the value in quotes for SQL
                target_cols.append(target_col)  # Add target column to be inserted

        if values:  # If there are valid values to insert
            # Create the SQL insert statement dynamically
            columns_part = ", ".join(f"{col}" for col in target_cols)
            values_part = ", ".join(values)
            insert_statement = f"INSERT INTO {target_table_name} ({columns_part}) VALUES ({values_part});"
            insert_statements.append(insert_statement)
            print(f"  Generated Insert Statement: {insert_statement}")
        else:
            print(f"  No values to insert for Source Row {index + 1}")

    # Return the final list of insert statements
    return insert_statements

# Example input





# Helper function to get the source column for a target column
def get_source_column_for_target(target_col, mappings):
    """Finds the source column corresponding to a target column."""
    for source_col, mapped_target_col in mappings.items():
        if mapped_target_col == target_col:
            return source_col
    return None

# Main function to handle dynamic mapping and write the output
@csrf_exempt
def mapping_data(request):
    if request.method != 'POST':
        return JsonResponse({"error": "POST request required."}, status=400)

    # Retrieve file names from session (dynamic file handling)
    
    uploaded_files = request.session.get('uploaded_files', {})

    # Retrieve file names from session
    source_file_name = uploaded_files.get('source_file')
    target_file_name = uploaded_files.get('target_file')

    if not source_file_name or not target_file_name:
        return JsonResponse({"error": "Files not found in session. Please upload the files first."}, status=400)

    # Define the upload directory (dynamic file paths)
    upload_dir = os.path.join(settings.MEDIA_ROOT, 'uploads')

    # Get dynamic file paths from session
    source_file_path = os.path.join(upload_dir, source_file_name)
    target_file_path = os.path.join(upload_dir, target_file_name)

    if not os.path.exists(source_file_path) or not os.path.exists(target_file_path):
        return JsonResponse({"error": "Files do not exist on the server."}, status=400)

    # Read source file content
    with open(source_file_path, 'r') as file:
        source_sql_content = file.read()

    # Read target file content
    with open(target_file_path, 'r') as file:
        target_sql_content = file.read()

    # Extract columns, primary keys, and foreign keys from source and target dynamically
    source_columns, source_primary_keys, source_foreign_keys,source_table_name = extract_columns(source_sql_content)
    target_columns, target_primary_keys, target_foreign_keys,target_table_name = extract_columns(target_sql_content)

    # Assuming mappings are sent dynamically in request body
    data = json.loads(request.body)
    mappings = data.get('mappings', {})

    # Validate mappings dynamically
    for source_col, target_col in mappings.items():
        if source_col not in source_columns:
            return JsonResponse({"error": f"Source column '{source_col}' not found."}, status=400)
        if target_col not in target_columns:
            return JsonResponse({"error": f"Target column '{target_col}' not found."}, status=400)

    # Extract data from the dynamic source file
    source_data = extract_data_from_source(source_sql_content, list(mappings.keys()))
    print("Source Data: ", source_data)
    source_columns = extract_columns_from_create(source_sql_content)
    print("columns target",target_columns)
    print("mapping",mappings)
    print("source",source_columns)
    # mapping = get_mapped_columns(source_sql_content, mapping)
    # Extract data from the INSERT INTO statements in the source SQL file
    # source_data = extract_data_from_source(source_sql_content, source_columns)

    # Print extracted columns and data for debugging
    # print("Extracted Columns from Source: ", source_columns)
    # print("Extracted Data from Source: ", source_data)

    # Generate new INSERT statements for the dynamic target file based on the mappings
    target_insert_statements = generate_insert_statements(target_table_name,target_columns, source_data, mappings)
    print("statement",target_insert_statements)
    # Find the position to insert the data dynamically
    insert_position = target_sql_content.find('-- Dumping data for table destination')

    if insert_position != -1:
        # Split content to insert data dynamically right after the comment
        pre_insert_content = target_sql_content[:insert_position]
        post_insert_content = target_sql_content[insert_position:]

        # Create the insert statements block dynamically
        insert_statements_block = "\n".join(target_insert_statements) + "\n"
        print("insert block ",insert_statements_block)
        # Write the modified content back to the dynamic target file
        with open(target_file_path, 'w') as target_file:
            target_file.write(pre_insert_content + insert_statements_block + post_insert_content)

    return JsonResponse({
        "message": "Data copied successfully",
        "source_columns": source_columns,
        "target_columns": target_columns,
        "source_primary_keys": list(source_primary_keys),
        "target_primary_keys": list(target_primary_keys),
        "source_foreign_keys": source_foreign_keys,
        "target_foreign_keys": target_foreign_keys
    })

import re

# Function to extract column names from the CREATE TABLE statement
def extract_columns_from_create(sql_content):
    columns = []
    # Regex to match the CREATE TABLE statement
    create_table_pattern = r'CREATE TABLE\s+?\w+?\s*\((.*?)\)\s*ENGINE='
    match = re.search(create_table_pattern, sql_content, re.DOTALL)
    if match:
        column_defs = match.group(1).strip().split(',')
        for column_def in column_defs:
            column_match = re.match(r'?(\w+)?\s+', column_def.strip())
            if column_match:
                columns.append(column_match.group(1))
    return columns

# Function to extract data from the source SQL INSERT statements
def extract_data_from_source(sql_content, columns):
    """Extracts data from the source SQL file based on the columns."""
    data = []
    # Regex to capture the INSERT INTO statement and its values
    insert_pattern = re.compile(r"INSERT INTO ?\w+?\s*VALUES\s*(\(.*?\));", re.IGNORECASE | re.DOTALL)
    matches = insert_pattern.findall(sql_content)

    # Loop over each VALUES section
    for value_list in matches:
        # This handles multiple value rows in a single INSERT statement
        value_rows = re.findall(r'\((.*?)\)', value_list)
        for row in value_rows:
            values = [val.strip().strip("'") for val in row.split(',')]  # Stripping quotes and whitespace
            row_data = {}
            for col, val in zip(columns, values):
                row_data[col] = val
            data.append(row_data)
    return data

# Test with your source file content
sql_content = """Your full SQL content here"""

# Extract columns from the CREATE TABLE section
columns = extract_columns_from_create(sql_content)
print("Extracted Columns: ", columns)

# Extract data from the INSERT INTO statements
source_data = extract_data_from_source(sql_content, columns)
print("Extracted Data: ", source_data)
def get_mapped_columns(sql_content, mapped_columns):
    """Returns a mapping of the source columns to the specified mapped columns."""
    # Dictionary to hold the final mapping
    column_mapping = {}
    
    # Regex pattern to find CREATE TABLE statements and extract columns
    create_table_pattern = re.compile(
        r"CREATE TABLE ?\w+?\s*\((.*?)\);",
        re.IGNORECASE | re.DOTALL
    )
    
    # Extract column definitions from the SQL content
    create_table_matches = create_table_pattern.findall(sql_content)
    
    for column_def in create_table_matches:
        # Split the column definitions into individual columns
        columns = [col.strip().strip("`") for col in column_def.split(',')]
        
        # Check each column and map it to the specified mapped columns
        for col in columns:
            if col in mapped_columns:
                column_mapping[col] = col  # You can customize the mapping here if needed

    return column_mapping