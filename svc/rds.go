package svc

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/atsushi-ishibashi/slow-collector/model"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
)

type ListInstancesInput struct {
	Clusters []string
}

type ListSlowLogsInput struct {
	// requried
	Instance string
	// StartTime < EndTime
	StartTime time.Time
	// default is now
	EndTime time.Time
}

func (i ListSlowLogsInput) validate() error {
	if i.Instance == "" {
		return errors.New("ListSlowLogsInput.Instance empty")
	}
	return nil
}

type GetLogDataInput struct {
	Instance string
	FileName string
}

func (i GetLogDataInput) validate() error {
	if i.Instance == "" {
		return errors.New("GetLogDataInput.Instance empty")
	}
	if i.FileName == "" {
		return errors.New("GetLogDataInput.FileName empty")
	}
	return nil
}

type RDSService interface {
	GetLogData(input GetLogDataInput) (string, error)
	ListInstances(input ListInstancesInput) ([]string, error)
	ListSlowLogs(input ListSlowLogsInput) ([]model.DBLogFile, error)
}

type rdsService struct {
	svc rdsiface.RDSAPI
}

func NewRDSService() RDSService {
	return &rdsService{
		svc: rds.New(session.New(), aws.NewConfig().WithRegion(os.Getenv("_SC_AWS_REGION"))),
	}
}

func (s *rdsService) ListInstances(input ListInstancesInput) ([]string, error) {
	if len(input.Clusters) > 0 {
		result := make([]string, 0)
		for _, cluster := range input.Clusters {
			r, err := s.listCluserInstances(cluster)
			if err != nil {
				return nil, err
			}
			result = append(result, r...)
		}
		return result, nil
	}
	return s.listAllInstances()
}

func (s *rdsService) listCluserInstances(cluster string) ([]string, error) {
	result := make([]string, 0)
	input := &rds.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(cluster),
	}
	resp, err := s.svc.DescribeDBClusters(input)
	if err != nil {
		return nil, err
	}
	for _, v := range resp.DBClusters {
		for _, vv := range v.DBClusterMembers {
			if vv.DBInstanceIdentifier != nil {
				result = append(result, *vv.DBInstanceIdentifier)
			}
		}
	}
	return result, nil
}

func (s *rdsService) listAllInstances() ([]string, error) {
	result := make([]string, 0)
	err := s.svc.DescribeDBInstancesPages(&rds.DescribeDBInstancesInput{},
		func(page *rds.DescribeDBInstancesOutput, lastPage bool) bool {
			for _, v := range page.DBInstances {
				if v.DBInstanceIdentifier != nil {
					result = append(result, *v.DBInstanceIdentifier)
				}
			}
			return page.Marker != nil
		})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *rdsService) ListSlowLogs(input ListSlowLogsInput) ([]model.DBLogFile, error) {
	if err := input.validate(); err != nil {
		return nil, err
	}
	param := &rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: aws.String(input.Instance),
		FileSize:             aws.Int64(0),
		FilenameContains:     aws.String("slowquery/mysql-slowquery.log."),
	}
	if !input.StartTime.IsZero() {
		startEpoch := input.StartTime.Unix() * 1000
		param.SetFileLastWritten(startEpoch)
	}
	var endEpoch int64
	if !input.EndTime.IsZero() {
		endEpoch = input.EndTime.Unix() * 1000
	}
	result := make([]model.DBLogFile, 0)
	err := s.svc.DescribeDBLogFilesPages(param,
		func(page *rds.DescribeDBLogFilesOutput, lastPage bool) bool {
			for _, v := range page.DescribeDBLogFiles {
				if endEpoch != 0 && v.LastWritten != nil && endEpoch < *v.LastWritten {
					continue
				}
				if v.LogFileName != nil {
					result = append(result, model.DBLogFile{
						Name:     *v.LogFileName,
						Instance: input.Instance,
					})
				}
			}
			return page.Marker != nil
		})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *rdsService) GetLogData(input GetLogDataInput) (string, error) {
	if err := input.validate(); err != nil {
		return "", err
	}
	param := &rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: aws.String(input.Instance),
		LogFileName:          aws.String(input.FileName),
	}
	var buff strings.Builder
	err := s.svc.DownloadDBLogFilePortionPages(param,
		func(page *rds.DownloadDBLogFilePortionOutput, lastPage bool) bool {
			if page.LogFileData != nil {
				buff.WriteString(*page.LogFileData)
			}
			return page.Marker != nil
		})
	if err != nil {
		return "", err
	}
	return buff.String(), nil
}
