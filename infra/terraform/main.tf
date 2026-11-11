# Bônus: infraestrutura como código. Recria do zero a mesma infraestrutura do init-ministack.sh.
#   MiniStack:  terraform init && terraform apply -auto-approve
#   AWS real:   terraform apply -var 'endpoint=' -var 'usar_ministack=false'

terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

variable "endpoint" {
  type    = string
  default = "http://localhost:4566"
}
variable "usar_ministack" {
  type    = bool
  default = true
}
variable "region" {
  type    = string
  default = "us-east-1"
}
variable "max_receive_count" {
  type    = number
  default = 3
}

provider "aws" {
  region                      = var.region
  access_key                  = var.usar_ministack ? "test" : null
  secret_key                  = var.usar_ministack ? "test" : null
  skip_credentials_validation = var.usar_ministack
  skip_metadata_api_check     = var.usar_ministack
  skip_requesting_account_id  = var.usar_ministack
  s3_use_path_style           = var.usar_ministack

  dynamic "endpoints" {
    for_each = var.usar_ministack ? [1] : []
    content {
      sns = var.endpoint
      sqs = var.endpoint
      s3  = var.endpoint
      ssm = var.endpoint
    }
  }
}

locals {
  filas = {
    "reservas-queue"     = { visibilidade = 30, tipos = ["VeiculoBloqueado", "VeiculoIndisponivel", "PagamentoAprovado", "PagamentoRecusado"] }
    "frota-queue"        = { visibilidade = 30, tipos = ["ReservaSolicitada", "ReservaConfirmada", "ReservaCancelada"] }
    "pagamentos-queue"   = { visibilidade = 60, tipos = ["PagamentoSolicitado", "EstornoSolicitado"] }
    "notificacoes-queue" = { visibilidade = 30, tipos = ["ReservaConfirmada", "ReservaCancelada", "PagamentoEstornado"] }
  }
}

resource "aws_sns_topic" "eventos" {
  name = "locacao-eventos"
}

resource "aws_sqs_queue" "dlq" {
  for_each                  = local.filas
  name                      = replace(each.key, "-queue", "-dlq")
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "fila" {
  for_each                   = local.filas
  name                       = each.key
  visibility_timeout_seconds = each.value.visibilidade
  receive_wait_time_seconds  = 10
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq[each.key].arn
    maxReceiveCount     = var.max_receive_count
  })
}

resource "aws_sqs_queue_policy" "sns_pode_enviar" {
  for_each  = local.filas
  queue_url = aws_sqs_queue.fila[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "sns.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.fila[each.key].arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_sns_topic.eventos.arn } }
    }]
  })
}

resource "aws_sns_topic_subscription" "assinatura" {
  for_each             = local.filas
  topic_arn            = aws_sns_topic.eventos.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.fila[each.key].arn
  raw_message_delivery = true
  filter_policy        = jsonencode({ type = each.value.tipos })
}

resource "aws_s3_bucket" "contratos" {
  bucket        = "locacao-contratos"
  force_destroy = true
}

resource "aws_ssm_parameter" "params" {
  for_each = {
    "/locacao/sns/topico-eventos-arn"              = aws_sns_topic.eventos.arn
    "/locacao/reservas/timeout-pagamento-segundos" = "30"
    "/locacao/reservas/precos-diaria"              = jsonencode({ HATCH = 120, SEDAN = 180, SUV = 250 })
    "/locacao/pagamentos/max-tentativas"           = "3"
  }
  name      = each.key
  type      = "String"
  value     = each.value
  overwrite = true
}

resource "aws_ssm_parameter" "gateway_api_key" {
  name      = "/locacao/gateway/api-key"
  type      = "SecureString"
  value     = "gw-chave-dev"
  overwrite = true
}

output "topic_arn" { value = aws_sns_topic.eventos.arn }
output "filas" { value = { for k, q in aws_sqs_queue.fila : k => q.url } }
