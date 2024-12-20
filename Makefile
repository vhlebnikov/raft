# go mod vendor !!!
# go build
# docker run golang image with bin entrypoint on start
# 
#

.PHONY: build
build:
	go build -v -o ./bin/node ./cmd