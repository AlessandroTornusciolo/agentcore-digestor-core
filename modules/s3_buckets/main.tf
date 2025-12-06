resource "aws_s3_bucket" "s3_bucket" {
  for_each = var.buckets

  bucket = each.value.name

  tags = merge(
    {
      Project     = "agentcore"
      Module      = "digestor"
      Environment = var.env
    },
    each.value.tags
  )
}

resource "aws_s3_bucket_versioning" "s3_bucket_versioning" {
  for_each = {
    for key, value in var.buckets :
    key => value
    if value.enable_versioning
  }

  bucket = aws_s3_bucket.s3_bucket[each.key].id

  versioning_configuration {
    status = each.value.versioning_status
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "s3_bucket_sse" {
  for_each = {
    for key, value in var.buckets :
    key => value
    if value.enable_encryption
  }

  bucket = aws_s3_bucket.s3_bucket[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = each.value.encryption_type
    }
  }
}

resource "aws_s3_bucket_policy" "bucket_policy" {
  for_each = {
    for key, value in var.buckets :
    key => value
    if contains(keys(value), "policy") && value.policy != null
  }

  bucket = aws_s3_bucket.s3_bucket[each.key].id
  policy = each.value.policy
}
