variable "credentials" {
  description = "The credentials of the project"
  default = "./keys/mycreds.json"
}

variable "project_id" {
  description = "The ID of the project"
  default = "basic-decoder-415403"
}

variable "bq_dataset_name" {
    description = "The name of the BigQuery dataset"
    default = "french_it_jobs"
}

variable "gcs_storage_class" {
  description = "The storage class of the GCS bucket"
  default = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "The name of the GCS bucket"
  default = "french-it-jobs-lake"
}

variable "location" {
  description = "The location of the GCS bucket"
  default = "europe-west9"
}

variable "region" {
  description = "The region of the GCS bucket"
  default = "europe-west9"
}