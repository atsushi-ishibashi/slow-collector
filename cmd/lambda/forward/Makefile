build:
	GOOS=linux GOARCH=amd64 go build -ldflags "-s -w" -o main
	zip main.zip main

deploy:
	aws lambda update-function-code \
	--function-name ${function} \
	--zip-file fileb://main.zip \
	--profile ${profile} \
	--region ap-northeast-1
