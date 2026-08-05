resource "aws_glue_catalog_database" "bronze" {
  name        = "enterprise_etl_dev_bronze"
  description = "Bronze-layer metadata for raw ingested data"

  location_uri = "s3://${aws_s3_bucket.lakehouse.id}/bronze/"
}

resource "aws_glue_catalog_database" "silver" {
  name        = "enterprise_etl_dev_silver"
  description = "Silver-layer metadata for raw ingested data"

  location_uri = "s3://${aws_s3_bucket.lakehouse.id}/silver/"
}

resource "aws_glue_catalog_database" "gold" {
  name        = "enterprise_etl_dev_gold"
  description = "Gold-layer metadata for raw ingested data"

  location_uri = "s3://${aws_s3_bucket.lakehouse.id}/gold/"
}

