locals {
  glue_execution_role_name = "AWSGlueServiceRole-${var.project_name}-${var.environment}"
}

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    sid     = "AllowGlueToAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_execution" {
  name               = local.glue_execution_role_name
  description        = "Execution role for ETL Glue jobs"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

#custom permissions for our specific lakehouse
data "aws_iam_policy_document" "glue_lakehouse_access" {
  statement {
    sid    = "LakehouseBucketAccess"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads"
    ]

    resources = [
      aws_s3_bucket.lakehouse.arn
    ]
  }

  statement {
    sid    = "LakehouseObjectAccess"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]

    resources = [
      "${aws_s3_bucket.lakehouse.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "glue_lakehouse_access" {
  name        = "${var.project_name}-${var.environment}-glue-lakehouse-access"
  description = "Allows Glue jobs to read and write the project lakehouse bucket."
  policy      = data.aws_iam_policy_document.glue_lakehouse_access.json
}

# Standard Glue service permissions:
# Glue APIs, CloudWatch logs and metrics, and Glue networking operations.
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Access to our specific S3 lakehouse bucket.
resource "aws_iam_role_policy_attachment" "glue_lakehouse_access" {
  role       = aws_iam_role.glue_execution.name
  policy_arn = aws_iam_policy.glue_lakehouse_access.arn
}
