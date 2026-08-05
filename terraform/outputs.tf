output "aws_account_id" {
  description = "AWS account authenticated by Terraform."
  value       = data.aws_caller_identity.current.account_id
}

output "aws_caller_arn" {
  description = "IAM identity authenticated by Terraform."
  value       = data.aws_caller_identity.current.arn
}

output "aws_region" {
  description = "AWS region used by Terraform."
  value       = var.aws_region
}

output "lakehouse_bucket_name" {
  description = "Name of the S3 lakehouse bucket"
  value       = aws_s3_bucket.lakehouse.id
}

output "lakehouse_bucket_arn" {
  description = "ARN of the S3 lakehouse bucket"
  value       = aws_s3_bucket.lakehouse.arn
}

output "lakehouse_s3_uri" {
  description = "S3 URI of the lakehouse bucket"
  value       = "s3://${aws_s3_bucket.lakehouse.id}"
}

output "glue_bronze_database" {
  description = "Bronze Glue Data catalog database"
  value       = aws_glue_catalog_database.bronze
}

output "glue_silver_database" {
  description = "Silver Glue Data catalog database"
  value       = aws_glue_catalog_database.silver
}

output "glue_gold_database" {
  description = "Gold Glue Data catalog database"
  value       = aws_glue_catalog_database.gold
}

output "glue_execution_role_name" {
  description = "Name of the AWS Glue execution role."
  value       = aws_iam_role.glue_execution.name
}

output "glue_execution_role_arn" {
  description = "ARN of the AWS Glue execution role."
  value       = aws_iam_role.glue_execution.arn
}

output "glue_lakehouse_policy_arn" {
  description = "ARN of the Glue lakehouse S3 access policy."
  value       = aws_iam_policy.glue_lakehouse_access.arn
}