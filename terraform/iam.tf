resource "aws_iam_user" "citibike_pipeline" {
    name = "citibike-pipeline"
}

resource "aws_iam_policy" "citibike_s3_access" {
    name = "citibike-s3-access"
    description = "Awwlos read/write access to the citibike s3 bucket"

    policy = jsonencode({
        Version = "2012-10-17"
        Statement = [
            {
                Effect = "Allow"
                Action = [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:DeleteObject"
                ]
                Resource = [
                    aws_s3_bucket.citibike.arn,
                    "${aws_s3_bucket.citibike.arn}/*"
                ]
            }
        ]
    })
}

resource "aws_iam_user_policy_attachment" "citibike" {
    user = aws_iam_user.citibike_pipeline.name
    policy_arn = aws_iam_policy.citibike_s3_access.arn
}