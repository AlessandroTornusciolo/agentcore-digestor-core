module "agentcore_s3_buckets" {
  source  = "./modules/s3_buckets"

  env     = var.env
  buckets = var.s3_buckets
}

module "agentcore_iam_roles" {
  source = "./modules/iam_roles"

  env   = var.env
  roles = var.iam_roles
}

module "agentcore_ecr_repositories" {
  source      = "./modules/ecr_repositories"
  env         = var.env
  repositories = var.ecr_repositories
}

module "agentcore_glue_catalog" {
  source = "./modules/glue_catalog"
  env    = var.env
}

module "agentcore_lambda_layers" {
  source = "./modules/lambda_layers"
  env    = var.env
  layers = var.layers
}

module "agentcore_lambda_functions" {
  source = "./modules/lambda_functions"
  env    = var.env

  lambdas = {
    for lambda_key, lambda_def in var.lambdas :
    lambda_key => merge(
      # REMOVE layer_names before passing into module
      { for k, v in lambda_def : k => v if k != "layer_names" },
      {
        role_arn = module.agentcore_iam_roles.role_arns[lambda_key]

        # Layers are only for ZIP Lambdas
        layers = (
          lambda_def.package_type == "Zip"
          ? [
              for lname in lookup(lambda_def, "layer_names", []) :
              module.agentcore_lambda_layers.layer_arns[lname]
              if can(module.agentcore_lambda_layers.layer_arns[lname])
            ]
          : []
        )
      }
    )
  }
}

module "agentcore_api_gateway" {
  source = "./modules/api_gateway"
  env    = var.env

  lambda_core_arn        = module.agentcore_lambda_functions.lambda_arns["lambda_core"]
  lambda_core_invoke_arn = module.agentcore_lambda_functions.lambda_arns["lambda_core"]
}


