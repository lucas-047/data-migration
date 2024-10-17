from django.shortcuts import render, HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import os
from django.conf import settings


@csrf_exempt
def upload_sql_file(request):
    # Define the upload directory
    upload_dir = os.path.join(settings.MEDIA_ROOT, 'uploads')

    # Ensure the upload directory exists
    os.makedirs(upload_dir, exist_ok=True)

    # Get the uploaded files
    source_file = request.FILES.get('source_file')
    target_file = request.FILES.get('target_file')

    if not source_file or not target_file:
        return JsonResponse({"error": "Both source and target SQL files are required."}, status=400)

    # Save files to the upload directory
    source_file_path = os.path.join(upload_dir, source_file.name)
    target_file_path = os.path.join(upload_dir, target_file.name)

    with open(source_file_path, 'wb+') as destination:
        for chunk in source_file.chunks():
            destination.write(chunk)

    with open(target_file_path, 'wb+') as destination:
        for chunk in target_file.chunks():
            destination.write(chunk)

    # Save the file names in the session
    request.session['uploaded_files'] = {
        'source_file': source_file.name,
        'target_file': target_file.name
    }

    return JsonResponse({"message": "Files uploaded successfully."})
