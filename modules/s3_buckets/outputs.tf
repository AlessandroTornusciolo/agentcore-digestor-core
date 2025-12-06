output "bucket_ids" {
  description = "Created S3 bucket IDs"
  value       = { for k, b in aws_s3_bucket.s3_bucket : k => b.id }
}
