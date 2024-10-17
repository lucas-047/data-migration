from django.shortcuts import render, HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
import re
import os
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json

# @csrf_exempt
# def migrate_data(request):
#     if request.method == 'POST':
#         try:
#             data = json.loads(request.body)
#             source_table_name = data.get('source_table_name')
#             target_table_name = data.get('target_table_name')
#             column_mapping = data.get('column_mapping')  # User-provided mapping
#         except json.JSONDecodeError:
#             return JsonResponse({"error": "Invalid JSON."}, status=400)

#         # Ensure the user provides a source table name, target table name, and at least one column mapping
#         if not source_table_name or not target_table_name or not column_mapping:
#             return JsonResponse({"error": "Source table name, target table name, and column mapping are required."}, status=400)

#         # Ensure at least one column is mapped
#         if not isinstance(column_mapping, dict) or len(column_mapping) == 0:
#             return JsonResponse({"error": "At least one column must be mapped."}, status=400)

#         # Build the query for the mapped columns only (ignoring unmapped ones)
#         mapped_source_columns = []
#         mapped_target_columns = []

#         for target_col, source_col in column_mapping.items():
#             # Only include columns that are mapped
#             mapped_source_columns.append(source_col)
#             mapped_target_columns.append(target_col)

#         # Generate the SQL query for the mapped columns
#         source_columns_str = ', '.join(mapped_source_columns)
#         target_columns_str = ', '.join(mapped_target_columns)

#         if len(mapped_source_columns) == 0 or len(mapped_target_columns) == 0:
#             return JsonResponse({"error": "At least one column must be mapped."}, status=400)

#         # Assuming you're copying data using an INSERT INTO SELECT pattern
#         insert_query = f"INSERT INTO {target_table_name} ({target_columns_str}) SELECT {source_columns_str} FROM {source_table_name};"

#         # Here you would typically execute the generated query, but for now, we'll just return it.
#         return JsonResponse({
#             "generated_query": insert_query,
#             "mapped_columns": column_mapping,
#         })

#     return JsonResponse({"error": "POST request expected."}, status=405)
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db import transaction, connections
import re
import json

@csrf_exempt
def migrate_data(request):
    # Ensure the request is a POST request
    if request.method != 'POST':
        return JsonResponse({"error": "Invalid request method."}, status=405)

    # Get the uploaded files
    source_file = request.FILES.get('source_file')
    target_file = request.FILES.get('target_file')

    if not source_file or not target_file:
        return JsonResponse({"error": "Both source and target SQL files are required."}, status=400)

    # Parse the JSON body for mappings
    try:
        data = json.loads(request.body)
        mappings = data.get('mappings', {})
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON format."}, status=400)

    # Function to extract columns and types from the SQL content
    def extract_columns(sql_content):
        columns_with_types = {}
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)  # Remove comments

        # Regex pattern for CREATE TABLE
        create_table_pattern = r'CREATE TABLE\s+`?\w+`?\s*\((.*?)\);'
        
        # Find all CREATE TABLE statements
        matches = re.findall(create_table_pattern, sql_content, re.IGNORECASE | re.DOTALL)
        for columns_definition in matches:
            column_defs = columns_definition.split(',')
            for column_def in column_defs:
                match = re.match(r'`?(\w+)`?\s+(\w+(\(\d+,\d+\)|\(\d+\)|)?)', column_def.strip())
                if match:
                    column_name = match.group(1)
                    data_type = match.group(2)
                    columns_with_types[column_name] = data_type

        return columns_with_types

    # Read and parse the source SQL file
    source_sql_content = source_file.read().decode('utf-8')
    source_columns = extract_columns(source_sql_content)

    # Read and parse the target SQL file
    target_sql_content = target_file.read().decode('utf-8')
    target_columns = extract_columns(target_sql_content)

    # Log the source and target columns for debugging
    print("Source Columns:", source_columns)
    print("Target Columns:", target_columns)

    # Begin data migration process
    with transaction.atomic(using='target_db'):  # Specify target database here
        for source_column, target_column in mappings.items():
            if source_column not in source_columns:
                return JsonResponse({"error": f"Source column '{source_column}' not found."}, status=400)
            if target_column not in target_columns:
                return JsonResponse({"error": f"Target column '{target_column}' not found."}, status=400)

            # Fetch data from the source database
            source_data_query = f"SELECT {source_column} FROM source_table_name"  # Update with your actual table name
            with connections['source_db'].cursor() as cursor:
                cursor.execute(source_data_query)
                source_data = cursor.fetchall()  # Fetch all data from source column

            # Insert data into the target database
            for row in source_data:
                insert_query = f"INSERT INTO target_table_name ({target_column}) VALUES (%s)"  # Update with your actual table name
                with connections['target_db'].cursor() as cursor:
                    cursor.execute(insert_query, [row[0]])  # Assuming row is a tuple

    return JsonResponse({"status": "Data migration successful!"})
