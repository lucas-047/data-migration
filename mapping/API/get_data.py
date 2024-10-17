from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import os
import re
import json
from django.conf import settings
from .parseSql import parse_sql_file,parse_mssql_file

import re


@csrf_exempt
def get_data(request):
    if request.method == 'GET':
        # Assuming you're reading the SQL content from the uploaded file

        uploaded_files = request.session.get('uploaded_files', {})
        source_file = uploaded_files.get('source_file')
        target_file = uploaded_files.get('target_file')

        if not source_file or not target_file:
            return JsonResponse({"error": "No uploaded files found in session."}, status=400)

        upload_dir = os.path.join(settings.MEDIA_ROOT, 'uploads')
        source_file_path = os.path.join(upload_dir, source_file)
        target_file_path = os.path.join(upload_dir, target_file)

        try:
            with open(source_file_path, 'r', encoding='utf-16') as source_file:
                sql_content = source_file.read()
        except FileNotFoundError:
            return JsonResponse({"error": "Source file not found."}, status=404)

        try:
            with open(target_file_path, 'r', encoding='utf-8') as target_file:
                target_content = target_file.read()
        except FileNotFoundError:
            return JsonResponse({"error": "Target file not found."}, status=404)

        # sql_content = request.FILES['source_file'].read().decode('utf-8')
        # target_content = request.FILES['target_file'].read().decode('utf-8')

        # Extract the metadata for all tables
        source_tables_content = parse_mssql_file(sql_content)
        target_tables_content = parse_sql_file(target_content)


        # Prepare the JSON response
        response_data = {
            "response": "SQL Parsed Successfully",
            "sourceData": source_tables_content,
            "targetData": target_tables_content
        }

        json_response = json.dumps(response_data, indent=4)

        # Return the response as JSON
        return JsonResponse(json_response,safe=False)

    return JsonResponse({'error': 'Invalid request method'}, status=400)
