package s3

// Note: Client construction is the caller's responsibility.
// Use the AWS SDK directly to create an S3 client:
//
//	import (
//	    "github.com/aws/aws-sdk-go-v2/config"
//	    "github.com/aws/aws-sdk-go-v2/service/s3"
//	)
//
//	cfg, _ := config.LoadDefaultConfig(ctx)
//	client := s3.NewFromConfig(cfg)
//
// For S3-compatible services (MinIO, LocalStack), configure the endpoint:
//
//	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
//	    o.BaseEndpoint = aws.String("http://localhost:4566")
//	    o.UsePathStyle = true
//	})
