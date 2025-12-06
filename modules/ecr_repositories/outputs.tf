output "repository_urls" {
  description = "Map of repository URLs by key"
  value = {
    for k, r in aws_ecr_repository.repo :
    k => r.repository_url
  }
}

output "repository_arns" {
  description = "Map of repository ARNs by key"
  value = {
    for k, r in aws_ecr_repository.repo :
    k => r.arn
  }
}
