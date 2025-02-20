provider "google" {
  project = "PROYECTO GCP"
  region  = "REGON"
}

resource "google_storage_bucket_object" "source" {
  name   = "function-source.zip"
  bucket = "BUCKET DEL PROYECTO"
  source = "function-source.zip"
}

resource "google_cloudfunctions_function" "function" {
  name        = "my-function"
  runtime     = "python310"
  region      = "REGION"
  entry_point = "hello_world"

  source_archive_bucket = "BUCKE DEL PROYECTO"
  source_archive_object = google_storage_bucket_object.source.name

  trigger_http = true
}

