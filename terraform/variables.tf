variable "aws_region" {
  description = "AWS region for project resources."
  type        = string
  default     = "ap-south-1"
}

variable "project_name" {
  description = "Project name used for tagging AWS resources."
  type        = string
  default     = "enterprise-etl-modernization"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"
}