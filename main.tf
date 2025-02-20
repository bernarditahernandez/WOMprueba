provider "google" {
  project = "projectwombh192025"
  region  = "us-central1"
}

resource "google_storage_bucket_object" "source" {
  name   = "function-source.zip"
  bucket = "bucketwombh192025"
  source = "function-source.zip"
}

resource "google_cloudfunctions_function" "function" {
  name        = "my-function"
  runtime     = "python310"
  region      = "us-central1"
  entry_point = "hello_world"

  source_archive_bucket = "bucketwombh192025"
  source_archive_object = google_storage_bucket_object.source.name

  trigger_http = true
}

