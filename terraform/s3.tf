resource "aws_s3_bucket" "citibike" {
    bucket = "terry-citibike-pipeline"
}

resource "aws_s3_bucket_public_access_block" "citibike" {
    bucket = aws_s3_bucket.citibike.id

    block_public_acls = true
    block_public_policy = true
    ignore_public_acls = true
    restrict_public_buckets = true 
}
