.PHONY: build
build:
	go build -v -o ./bin/node ./cmd

.PHONY: run
run:
	./bin/node -node 1 -http :8080 -cluster "1,:9090;2,:9091;3,:9092;4,:9093;5,:9094"
	./bin/node -node 2 -http :8081 -cluster "1,:9090;2,:9091;3,:9092;4,:9093;5,:9094"
	./bin/node -node 3 -http :8082 -cluster "1,:9090;2,:9091;3,:9092;4,:9093;5,:9094"
	./bin/node -node 4 -http :8083 -cluster "1,:9090;2,:9091;3,:9092;4,:9093;5,:9094"
	./bin/node -node 5 -http :8084 -cluster "1,:9090;2,:9091;3,:9092;4,:9093;5,:9094"
