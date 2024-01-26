variable "credentials" {
  description = "My GCP Credentials"
  default     = "./keys/dezc-brett-7455d044a2fa.json"
}

variable "project" {
  description = "GCP Project"
  default     = "dezc-brett"
}

variable "location" {
  description = "GCP Project Location"
  default     = "US"
}

variable "region" {
  description = "GCP Region"
  default     = "us-central1"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcp_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "dezc-brett-terra-bucket"
}

variable "gcp_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

