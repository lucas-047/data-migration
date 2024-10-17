from django.urls import path
from .API.fileUpload import upload_sql_file
from .API.migrate_data import migrate_data
from .API.get_data import get_data
from .API.migrate import mapping_data

urlpatterns = [
    path("api/upload-sql/", upload_sql_file, name="File Upload"),
    path("api/migrate-data/", migrate_data, name="Migrate"),
    path("api/get-data/", get_data, name="Fetch Data"),
    path("api/map", mapping_data, name="Map Column"),
]
