package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/atsushi-ishibashi/slow-collector/svc"
)

var (
	dir   = flag.String("dir", "", "directory to put logfiles")
	start = flag.String("start", time.Now().AddDate(0, 0, -1).Format("2006-01-02T03:04:05"), "start time to collect logs, UTC")
	end   = flag.String("end", time.Now().Format("2006-01-02T03:04:05"), "start time to collect logs, UTC")

	startTime, endTime time.Time
)

type clustersFlags []string

func (i *clustersFlags) String() string {
	return "clusters you want to collect slow query logs"
}

func (i *clustersFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var clusterFlags clustersFlags

func main() {
	flag.Var(&clusterFlags, "cluster", "target clusters to collect slow query logs")
	flag.Parse()
	if *dir == "" {
		log.Fatalln("dir required")
	}
	if t, err := time.Parse("2006-01-02T03:04:05", *start); err != nil {
		log.Fatalln("-start: ", err)
	} else {
		startTime = t
	}
	if t, err := time.Parse("2006-01-02T03:04:05", *end); err != nil {
		log.Fatalln("-end: ", err)
	} else {
		endTime = t
	}

	region := os.Getenv("AWS_REGION")
	defaultRegion := os.Getenv("AWS_DEFAULT_REGION")
	if region != "" {
		os.Setenv("_SC_AWS_REGION", region)
	} else if defaultRegion != "" {
		os.Setenv("_SC_AWS_REGION", defaultRegion)
	} else {
		log.Fatalln("env AWS_REGION or AWS_DEFAULT_REGION required")
	}

	rdsSvc := svc.NewRDSService()
	instances, err := rdsSvc.ListInstances(svc.ListInstancesInput{Clusters: clusterFlags})
	if err != nil {
		log.Fatalln(err)
	}

	current := time.Now().Format("200601020304")
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
		createDir(fmt.Sprintf("%s/%s", *dir, inst))
		f, err := os.Create(fmt.Sprintf("%s/%s/%s.log", *dir, inst, current))
		if err != nil {
			log.Fatalln(err)
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
			scanner := bufio.NewScanner(strings.NewReader(data))
			for scanner.Scan() {
				text := scanner.Text()
				if strings.HasPrefix(text, "/rdsdbbin/oscar") {
					continue
				}
				if strings.HasPrefix(text, "Tcp port:") {
					continue
				}
				if strings.HasPrefix(text, "Time") {
					continue
				}
				f.WriteString(text)
				f.WriteString("\n")
			}
		}
		f.Close()
	}
}

func createDir(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0755)
	}
}
