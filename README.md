//init the project
go mod init kinesis_client

// to add the dependencies
go mod tidy

// to compile
go build -o .build/kinesis_client ./src
