# slow-collector
collects slow query logs in RDS

## Install
```
go get -u github.com/atsushi-ishibashi/slow-collector/cmd/slow-collector
```

## Usage
```
$ slow-collector -h
Usage of ./slow-collector:
  -cluster value
    	target clusters to collect slow query logs
  -dir string
    	directory to put logfiles
  -end string
    	start time to collect logs, UTC (default "2019-02-07T05:07:32")
  -start string
    	start time to collect logs, UTC (default "2019-02-06T05:07:32")
```
