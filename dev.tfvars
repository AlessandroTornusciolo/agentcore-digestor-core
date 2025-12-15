aws_profile = "default"
aws_region = "eu-central-1"
env        = "dev"

s3_buckets = {
  raw = {
    name              = "agentcore-digestor-raw-dev"
    tags              = { Purpose = "raw-storage" }

    enable_versioning = true
    versioning_status = "Enabled"

    enable_encryption = true
    encryption_type   = "AES256"
  }

  tables = {
    name              = "agentcore-digestor-tables-dev"
    tags              = { Purpose = "iceberg-tables-storage" }

    enable_versioning = true
    versioning_status = "Enabled"

    enable_encryption = true
    encryption_type   = "AES256"
  }

  athena_results = {
    name              = "agentcore-digestor-athena-results-dev"
    tags              = { Purpose = "athena-results-storage" }

    enable_versioning = true
    versioning_status = "Enabled"

    enable_encryption = true
    encryption_type   = "AES256"

    policy = <<EOF
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Sid": "AllowAthenaAccess",
          "Effect": "Allow",
          "Principal": {
            "Service": "athena.amazonaws.com"
          },
          "Action": [
            "s3:GetBucketLocation",
            "s3:ListBucket",
            "s3:GetObject",
            "s3:PutObject"
          ],
          "Resource": [
            "arn:aws:s3:::agentcore-digestor-athena-results-dev",
            "arn:aws:s3:::agentcore-digestor-athena-results-dev/*"
          ]
        }
      ]
    }
    EOF
  }

  history = {
    name              = "agentcore-digestor-archive-dev"
    tags              = { Purpose = "archive-storage" }

    enable_versioning = true
    versioning_status = "Enabled"

    enable_encryption = true
    encryption_type   = "AES256"
  }

  manual_ingestion = {
    name              = "agentcore-digestor-upload-raw-dev"
    tags              = { Purpose = "user-manual-upload" }

    enable_versioning = true
    versioning_status = "Enabled"

    enable_encryption = true
    encryption_type   = "AES256"
  }

  iceberg = {
    name              = "agentcore-digestor-iceberg-bronze-dev"
    tags              = { Purpose = "iceberg-tables-storage" }

    enable_versioning = true
    versioning_status = "Enabled"

    enable_encryption = true
    encryption_type   = "AES256"
  }
}

iam_roles = {
  load_into_iceberg = {
    role_name       = "agentcore-digestor-role-load-into-iceberg-dev"
    assume_services = ["lambda.amazonaws.com"]

    inline_policies = {

      s3_read_raw = {
        policy_name = "agentcore-digestor-policy-s3-read-raw-load-into-iceberg-dev"
        statements = [
          {
            effect    = "Allow"
            actions   = ["s3:GetObject", "s3:ListBucket"]
            resources = [
              "arn:aws:s3:::agentcore-digestor-raw-dev",
              "arn:aws:s3:::agentcore-digestor-raw-dev/*",
              "arn:aws:s3:::agentcore-digestor-archive-dev",
              "arn:aws:s3:::agentcore-digestor-archive-dev/*",
              "arn:aws:s3:::agentcore-digestor-iceberg-bronze-dev",
              "arn:aws:s3:::agentcore-digestor-iceberg-bronze-dev/*",
              "arn:aws:s3:::agentcore-digestor-upload-raw-dev",
              "arn:aws:s3:::agentcore-digestor-upload-raw-dev/*"
            ]
          }
        ]
      }

      s3_write_tables = {
        policy_name = "agentcore-digestor-policy-s3-write-tables-load-into-iceberg-dev"
        statements = [
          {
            effect    = "Allow"
            actions   = ["s3:PutObject", "s3:ListBucket"]
            resources = [
              "arn:aws:s3:::agentcore-digestor-tables-dev",
              "arn:aws:s3:::agentcore-digestor-tables-dev/*",
              "arn:aws:s3:::agentcore-digestor-raw-dev",
              "arn:aws:s3:::agentcore-digestor-raw-dev/*",
              "arn:aws:s3:::agentcore-digestor-archive-dev",
              "arn:aws:s3:::agentcore-digestor-archive-dev/*",
              "arn:aws:s3:::agentcore-digestor-iceberg-bronze-dev",
              "arn:aws:s3:::agentcore-digestor-iceberg-bronze-dev/*",
              "arn:aws:s3:::agentcore-digestor-upload-raw-dev",
              "arn:aws:s3:::agentcore-digestor-upload-raw-dev/*"
            ]
          }
        ]
      }

      logs = {
        policy_name = "agentcore-digestor-policy-logs-load-into-iceberg-dev"
        statements = [
          {
            effect    = "Allow"
            actions   = [
              "logs:CreateLogGroup",
              "logs:CreateLogStream",
              "logs:PutLogEvents"
            ]
            resources = ["*"]
          }
        ]
      }

      ecr_access = {
        policy_name = "agentcore-digestor-policy-ecr-access-load-into-iceberg-dev"
        statements = [
          {
            effect = "Allow"
            actions = [
              "ecr:GetDownloadUrlForLayer",
              "ecr:BatchGetImage",
              "ecr:BatchCheckLayerAvailability",
              "ecr:GetAuthorizationToken"
            ]
            resources = ["*"]
          }
        ]
      }
    }

    tags = { Purpose = "load-into-iceberg" }
  }
  iceberg_ctas = {
    role_name = "agentcore-digestor-role-lambda-iceberg-ctas-dev"
    assume_services = ["lambda.amazonaws.com"]

    inline_policies = {
      athena_glue_s3 = {
        policy_name = "athena-glue-s3-access"
        statements = [
          {
            effect = "Allow"
            actions = [
              "athena:StartQueryExecution",
              "athena:GetQueryExecution",
              "athena:GetQueryResults"
            ]
            resources = ["*"]
          },
          {
            effect = "Allow"
            actions = [
              "glue:CreateTable",
              "glue:DeleteTable",
              "glue:GetTable",
              "glue:GetTables",
              "glue:GetDatabase"
            ]
            resources = ["*"]
          },
          {
            effect = "Allow"
            actions = [
              "s3:GetBucketLocation",
              "s3:GetObject",
              "s3:ListBucket",
              "s3:PutObject"
            ]
            resources = [
              "arn:aws:s3:::agentcore-digestor-tables-dev/*",
              "arn:aws:s3:::agentcore-digestor-tables-dev",
              "arn:aws:s3:::aws-athena-query-results-*",
              "arn:aws:s3:::agentcore-digestor-athena-results-dev/*",
              "arn:aws:s3:::agentcore-digestor-athena-results-dev",         
              "arn:aws:s3:::agentcore-digestor-raw-dev",
              "arn:aws:s3:::agentcore-digestor-raw-dev/*",
              "arn:aws:s3:::agentcore-digestor-archive-dev",
              "arn:aws:s3:::agentcore-digestor-archive-dev/*",
              "arn:aws:s3:::agentcore-digestor-iceberg-bronze-dev",
              "arn:aws:s3:::agentcore-digestor-iceberg-bronze-dev/*",
              "arn:aws:s3:::agentcore-digestor-upload-raw-dev",
              "arn:aws:s3:::agentcore-digestor-upload-raw-dev/*"
            ]
          }
        ]
      }
    }

    tags = { Purpose = "iceberg-ctas" }
  }
  schema_normalizer = {
    role_name       = "agentcore-digestor-role-schema-normalizer-dev"
    assume_services = ["lambda.amazonaws.com"]

    inline_policies = {
      s3_read_tables = {
        policy_name = "schema-normalizer-s3"
        statements = [
          {
            effect = "Allow"
            actions = [
              "s3:GetObject",
              "s3:ListBucket"
            ]
            resources = [
              "arn:aws:s3:::agentcore-digestor-tables-dev",
              "arn:aws:s3:::agentcore-digestor-tables-dev/*",
              "arn:aws:s3:::agentcore-digestor-raw-dev",
              "arn:aws:s3:::agentcore-digestor-raw-dev/*",
              "arn:aws:s3:::agentcore-digestor-archive-dev",
              "arn:aws:s3:::agentcore-digestor-archive-dev/*",
              "arn:aws:s3:::agentcore-digestor-iceberg-bronze-dev",
              "arn:aws:s3:::agentcore-digestor-iceberg-bronze-dev/*",
              "arn:aws:s3:::agentcore-digestor-upload-raw-dev",
              "arn:aws:s3:::agentcore-digestor-upload-raw-dev/*"
            ]
          }
        ]
      }
    }

    tags = { Purpose = "schema-normalizer" }
  }
  bedrock_agent = {
    role_name       = "agentcore-digestor-role-bedrock-agent-dev"
    assume_services = ["bedrock.amazonaws.com"]

    inline_policies = {
      bedrock_model_invoke = {
        policy_name = "bedrock-agent-invoke-model"
        statements = [
          {
            effect = "Allow"
            actions = [
              "bedrock:InvokeModel",
              "bedrock:InvokeModelWithResponseStream"
            ]
            resources = ["*"] # se vuoi, poi la stringiamo sul singolo modello
          }
        ]
      }

      lambda_invoke = {
        policy_name = "bedrock-agent-lambda-invoke"
        statements = [
          {
            effect = "Allow"
            actions = [
              "lambda:InvokeFunction"
            ]
            resources = [
              # qui tutti gli ARN delle lambda che l’agent potrà usare come tool
              "arn:aws:lambda:eu-central-1:151441048511:function:agentcore-digestor-lambda-core-dev",
              # in futuro: lambda tool adapter, schema_normalizer, ecc.
            ]
          }
        ]
      }

      logs = {
        policy_name = "bedrock-agent-logs"
        statements = [
          {
            effect = "Allow"
            actions = [
              "logs:CreateLogGroup",
              "logs:CreateLogStream",
              "logs:PutLogEvents"
            ]
            resources = ["*"]
          }
        ]
      }
    }

    tags = {
      Purpose = "bedrock-agent"
    }
  }
  detect_file_type = {
    role_name       = "agentcore-digestor-role-detect-file-type-dev"
    assume_services = ["lambda.amazonaws.com"]

    inline_policies = {

      s3_read_input = {
        policy_name = "agentcore-digestor-policy-s3-read-input-detect-file-type-dev"
        statements = [
          {
            effect  = "Allow"
            actions = ["s3:GetObject", "s3:ListBucket"]
            resources = [
              "arn:aws:s3:::agentcore-digestor-upload-raw-dev",
              "arn:aws:s3:::agentcore-digestor-upload-raw-dev/*"
            ]
          }
        ]
      }

      logs = {
        policy_name = "agentcore-digestor-policy-logs-detect-file-type-dev"
        statements = [
          {
            effect = "Allow"
            actions = [
              "logs:CreateLogGroup",
              "logs:CreateLogStream",
              "logs:PutLogEvents"
            ]
            resources = ["*"]
          }
        ]
      }

      ecr_access = {
        policy_name = "agentcore-digestor-policy-ecr-access-detect-file-type-dev"
        statements = [
          {
            effect = "Allow"
            actions = [
              "ecr:GetDownloadUrlForLayer",
              "ecr:BatchGetImage",
              "ecr:BatchCheckLayerAvailability",
              "ecr:GetAuthorizationToken"
            ]
            resources = ["*"]
          }
        ]
      }
    }

    tags = { Purpose = "detect-file-type" }
  }
}

ecr_repositories = {
  load_into_iceberg = {
    component    = "load-into-iceberg"
    scan_on_push = true
    tags = {
      Purpose = "load-into-iceberg"
    }
  }
  schema_normalizer = {
    component = "schema-normalizer"
    scan_on_push = true
    tags = { 
      Purpose = "schema-normalizer" 
    }
  }
  detect_file_type = {
    component = "detect-file-type"
    scan_on_push = true
    tags = { 
      Purpose = "detect-file-type" 
    }
  }
}

layers = {}

lambdas = {
  load_into_iceberg = {
    function_name = "agentcore-digestor-lambda-load-into-iceberg-dev"
    package_type  = "Image"
    image_uri     = "151441048511.dkr.ecr.eu-central-1.amazonaws.com/agentcore-digestor-ecr-load-into-iceberg-dev:latest"
    timeout       = 60

    env_vars = { ENV = "dev" }
    tags     = { Purpose = "load-into-iceberg" }

    # Zip fields unused for image-based lambdas
    runtime       = null
    handler       = null
    source_path   = null
    layer_names   = []
  }
  iceberg_ctas = {
    function_name = "agentcore-digestor-lambda-iceberg-ctas-dev"
    package_type  = "Zip"
    handler       = "main.handler"
    runtime       = "python3.12"
    source_path   = "./dist/iceberg_ctas.zip"
    timeout       = 900   # CTAS può essere lento

    env_vars = { ENV = "dev" }
    tags     = { Purpose = "iceberg-ctas" }

    layer_names = []
  }
  schema_normalizer = {
    function_name = "agentcore-digestor-lambda-schema-normalizer-dev"

    package_type = "Image"
    image_uri = "151441048511.dkr.ecr.eu-central-1.amazonaws.com/agentcore-digestor-ecr-schema-normalizer-dev:latest"

    timeout = 60

    env_vars = {
      ENV = "dev"
    }

    tags = {
      Purpose = "schema-normalizer"
    }

    # unused in image mode
    runtime     = null
    handler     = null
    source_path = null
    layers      = null
  }
  detect_file_type = {
    function_name = "agentcore-digestor-lambda-detect-file-type-dev"
    package_type  = "Image"
    image_uri     = "151441048511.dkr.ecr.eu-central-1.amazonaws.com/agentcore-digestor-ecr-detect-file-type-dev:latest"
    timeout       = 60

    env_vars = { ENV = "dev" }
    tags     = { Purpose = "detect-file-type" }

    # Zip fields unused for image-based lambdas
    runtime       = null
    handler       = null
    source_path   = null
    layer_names   = []
  }
}
