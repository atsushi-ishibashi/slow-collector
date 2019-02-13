package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/atsushi-ishibashi/slow-collector/model"
	"github.com/atsushi-ishibashi/slow-collector/svc"
	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	lambda.Start(handler)
}

func handler() error {
	if os.Getenv("SC_AWS_REGION") == "" {
		return errors.New("env SC_AWS_REGION required")
	}
	os.Setenv("_SC_AWS_REGION", os.Getenv("SC_AWS_REGION"))

	if os.Getenv("SC_S3_BUCKET") == "" {
		return errors.New("env SC_S3_BUCKET required")
	}
	os.Setenv("_SC_S3_BUCKET", os.Getenv("SC_S3_BUCKET"))

	startTime := time.Now().AddDate(0, 0, -1)
	endTime := time.Now()

	rdsSvc := svc.NewRDSService()
	s3Svc := svc.NewS3Service()
	instances, err := rdsSvc.ListInstances(svc.ListInstancesInput{})
	if err != nil {
		return err
	}

	for _, inst := range instances {
		logs, err := rdsSvc.ListSlowLogs(svc.ListSlowLogsInput{
			Instance:  inst,
			StartTime: startTime,
			EndTime:   endTime,
		})
		if err != nil {
			log.Println("instance: ", inst, "  ", err)
			continue
		}

		for _, l := range logs {
			data, err := rdsSvc.GetLogData(svc.GetLogDataInput{
				Instance: l.Instance,
				FileName: l.Name,
			})
			if err != nil {
				log.Println("instance: ", l.Instance, ", LogFile: ", l.Name, "   ", err)
				continue
			}
			timeKey, err := constructFileKey(l)
			if err != nil {
				log.Println(err)
				continue
			}
			plfInput := svc.PutLogFileInput{
				Body: strings.NewReader(data),
				Key:  fmt.Sprintf("slowquery/%s/%s.log", l.Instance, timeKey),
			}
			if err := s3Svc.PutLogFile(plfInput); err != nil {
				log.Println(err)
				continue
			}
		}
	}
	return nil
}

func constructFileKey(ld model.DBLogFile) (string, error) {
	t, err := time.Parse("2006-01-02.03", strings.Replace(ld.Name, "slowquery/mysql-slowquery.log.", "", 1))
	if err != nil {
		return "", err
	}
	return t.Format("2006/01/02/03"), nil
}
