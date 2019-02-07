package svc

import (
	"errors"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type PutLogFileInput struct {
	Body io.ReadSeeker
	Key  string
}

func (i PutLogFileInput) validate() error {
	if i.Body == nil {
		return errors.New("PutLogFileInput.Body is nil")
	}
	if i.Key == "" {
		return errors.New("PutLogFileInput.Key is empty")
	}
	return nil
}

type S3Service interface {
	PutLogFile(input PutLogFileInput) error
}

type s3Service struct {
	svc s3iface.S3API
}

func NewS3Service() S3Service {
	return &s3Service{
		svc: s3.New(session.New(), aws.NewConfig().WithRegion(os.Getenv("_SC_AWS_REGION"))),
	}
}

func (s *s3Service) PutLogFile(input PutLogFileInput) error {
	param := &s3.PutObjectInput{
		Bucket:       aws.String(os.Getenv("_SC_S3_BUCKET")),
		Body:         input.Body,
		Key:          aws.String(input.Key),
		StorageClass: aws.String(s3.StorageClassStandardIa),
	}
	_, err := s.svc.PutObject(param)
	return err
}
