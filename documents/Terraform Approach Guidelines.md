# Terraform Approach Guidelines
## 1. Prerequisiti
* Terraform installato e funzionante.
* AWS CLI installata e configurata con credenziali valide.
* Accesso alla documentazione aggiornata dei servizi AWS coinvolti.

## 2. Organizzazione del progetto
Utilizzare una struttura chiara e modulare:
```bash
/terraform
  ├─ main.tf
  ├─ variables.tf
  ├─ outputs.tf
  ├─ providers.tf
  ├─ <env>.tfvars            # variabili locali (es. dev.tfvars)
  └─ modules/
      └─ <nome_modulo>/
          ├─ main.tf
          ├─ variables.tf
          └─ outputs.tf
```
Ogni risorsa complessa deve essere gestita tramite modulo (es. S3 buckets, IAM, VPC, ecc.).

## 3. Naming convention
* Tutti i nomi di risorse, moduli e variabili devono rispettare le regole del documento `agentcore_naming_convention.pdf`.
* In assenza di specifiche, usare naming leggibile, in snake_case, senza abbreviazioni inutili.

## 4. providers.tf (standard multi-environment)
```hcl
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}
```
Variabili necessarie:
```hcl
variable "aws_region"  { type = string }
variable "aws_profile" { type = string }
```

## 5. Gestione delle variabili `.tfvars`
* Ogni ambiente utilizza il proprio file:
    - `dev.tfvars`
    - `tst.tfvars`
    - `prd.tfvars`
* Per ora si utilizza solo `dev.tfvars`.
* Variabili complesse devono usare mappe (dizionari).

## 6. Filosofia dei moduli Terraform
* Devono permettere la creazione di N risorse con una singola chiamata, usando mappe + `for_each`.
* Le funzionalità extra devono essere opzionali (versioning, encryption ecc.).
* Devono essere:
    - riutilizzabili,
    - estensibili tramite flag,
    - privi di hard-coding superfluo.

### Modulo di esempio: s3_buckets
`main.tf`
```hcl
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
```

`variables.tf`
```hcl
variable "env" {
  type        = string
  description = "Environment (dev, test, prod)"
}

variable "buckets" {
  description = "Map of S3 buckets to create"

  type = map(object({
    name              = string
    tags              = map(string)

    enable_versioning = bool
    versioning_status = string     # Enabled / Suspended

    enable_encryption = bool
    encryption_type   = string     # AES256 / aws:kms
  }))
}
```

`outputs.tf`
```hcl
output "bucket_ids" {
  description = "Created S3 bucket IDs"
  value       = { for k, b in aws_s3_bucket.s3_bucket : k => b.id }
}
```

`dev.tfvars` **di esempio**
```hcl
env = "dev"

aws_region  = "eu-west-1"
aws_profile = "default"

buckets = {
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
}
```

## 7. IAM Policy (temporanea)
* Per ora è consentito l’uso di wildcard * nelle risorse IAM, per semplificare la fase iniziale.
* In seguito le policy saranno ottimizzate secondo il principio di least privilege.

## 8. Stile e qualità del codice
* Indentazione a 2 spazi.
* Evitare duplicazioni (local, mappe, moduli).
* Commentare solo se utile.
* Dichiarare sempre versioni (`required_version`, `required_providers`).

## 9. Output
* Esportare solo valori realmente utili.
* Non esporre mai informazioni sensibili.