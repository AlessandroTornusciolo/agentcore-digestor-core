output "runtime_ids" {
  description = "IDs of all created runtimes"
  value = {
    for k, r in aws_agentcore_runtime.runtime :
    k => r.runtime_id
  }
}

output "runtime_arns" {
  description = "ARNs of all created runtimes"
  value = {
    for k, r in aws_agentcore_runtime.runtime :
    k => r.arn
  }
}
