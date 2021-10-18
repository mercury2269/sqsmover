# sqsmover

A tool to move messages between SQS queues

## Compiling from source

You will need to have [Golang installed](https://golang.org/doc/install).

```sh
# clone it outside GOPATH
git clone https://github.com/rtkwlf/sqsmover
cd sqsmover

# get dependencies using go modules (needs go 1.11+)
go get ./...

# build
go build -o sqsmover .
```

## Run Instructions

To run sqsmover you must:

1. Clone the awn repo (if you haven't already)


2. Navigate to the awn repo


3. From awn repo folder, run `source setup.bash` (if you haven't already)


4. From any directory, run aws_creds_from_profile with the desired AWS profile

e.g. `aws_creds_from_profile dev2-AWN-Developer`

This command will print out three export commands:
 ```
export AWS_SESSION_TOKEN=[348 character base64 string]
export AWS_ACCESS_KEY_ID=[key id]
export AWS_SECRET_ACCESS_KEY=[40 character key]
```

5. Copy these commands and run them in the same session that you want to run sqsmover in

i.e. If you want to run sqsmover in bastion, run these export commands in bastion

6. Once these env vars are present you can run sqsmover like so:
```
./sqsmover -source ipusergrokker-deadletter-queue -destination ipusergrokker -region us-west-2
```

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| -source       |               | The source queue name to move messages from
| -destination  |               | The destination queue name to move messages to
| -region       | "us-west-2"   | [Optional] The AWS region for source and destination queues
| -limit        | 0             | [Optional] Limits total number of messages moved (No limit by default)
| -parallel     | 10            | [Optional] Max number of messages to be moved in parallel
