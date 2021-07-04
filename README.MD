# SQS Message Mover
[![Go Report Card](https://goreportcard.com/badge/github.com/mercury2269/sqsmover)](https://goreportcard.com/report/github.com/mercury2269/sqsmover) ![GitHub](https://img.shields.io/github/license/mercury2269/sqsmover)

`sqsmover` is a tool for moving AWS SQS messages from one queue to another. Useful when you need to move deadletter queue messages back into the original queue.

![sqs-mover-intro](https://user-images.githubusercontent.com/159128/75104229-721d7580-55bb-11ea-9b06-437353a174c0.gif)


## Features

* Reliable delivery. SQS Mover will only delete messages from the source queue after they were enqueued to the destination.
* Receives and sends messages in batches for faster processing.
* Progress indicator.
* User friendly info and error messages.
* Queue name resolution. For ease of use, you only need to provide a queue name and not the full `arn` address.
* Message attributes copy.
* Support for FIFO queues. MessageGroupId and MessageDeduplicationId are copied over to the destination messages.
* An optional flag to limit the number of messages to move.

## Installing

SQS Mover is pre-compiled for macOS, Linux, Windows and does not require additional dependencies. You can install
the pre-compiled binary (in several different ways) or compile from source.

**homebrew tap** (only on macOS for now):
```
brew install mercury2269/homebrew-tap/sqsmover
```

**Chocolatey (Windows)**
```
choco install sqsmover
```

**Shell script:**

The following script will install the binary into `/usr/local/bin`

```sh
$ curl https://raw.githubusercontent.com/mercury2269/sqsmover/master/install.sh | sh
```

Note that you may need to run the `sudo` version below, or alternatively chown `/usr/local/bin`:
```sh
$ curl https://raw.githubusercontent.com/mercury2269/sqsmover/master/install.sh | sudo sh
```

**From Source**

Download:
```sh
git clone git@github.com:mercury2269/sqsmover.git && cd sqsmover
```

Run:
```sh
AWS_PROFILE=nbos-ris AWS_REGION=us-west-2 go run main.go --source transactions-dlq --destination transactions
```

**Manually**:

Download the pre-compiled binary from the [releases page](https://github.com/mercury2269/sqsmover/releases) and
copy to the desired location.

## Set up AWS credentials 

Specifying AWS credentials in either a shared credentials file or environment variables. 

### Option 1: Creating the Credentials File

If you don’t have a shared credentials file (`~/.aws/credentials`), you can use any text editor to create one in your home directory. Add the following content to your credentials file, replacing `<YOUR_ACCESS_KEY_ID>` and `<YOUR_SECRET_ACCESS_KEY>` with your credentials.

```
[default]
aws_access_key_id = <YOUR_ACCESS_KEY_ID>
aws_secret_access_key = <YOUR_SECRET_ACCESS_KEY>
```

The [default] heading defines credentials for the default profile, which the SQSMover will use unless you configure it to use another profile. 

Optionally you can configure default region in `~/.aws/config`
```
[default]
region=us-west-2
```

### Option 2: Environment variables

As an alternative, you can setup AWS credentials in the environment variables.

The following examples show how to configure the environment variables.

Linux, OS X, or Unix

```
export AWS_ACCESS_KEY_ID=YOUR_AKID
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
```
Windows

```
set AWS_ACCESS_KEY_ID=YOUR_AKID
set AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
```

## Usage

```bash
sqsmover --help

usage: sqsmover --source=SOURCE --destination=DESTINATION [<flags>]

Flags:
  -h, --help                     Show context-sensitive help (also try
                                 --help-long and --help-man).
  -s, --source=SOURCE            The source queue name to move messages from.
  -d, --destination=DESTINATION  The destination queue name to move messages to.
  -r, --region="us-west-2"       The AWS region for source and destination queues.
  -e, --endpoint="https://..."   Use a specific endpoint in an AWS region. For more information see https://docs.aws.amazon.com/general/latest/gr/sqs-service.html
  -p, --profile=""               Use a specific profile from AWS credentials file.
  -l, --limit=0                  Limits total number of messages moved. No limit is set by default.
  -b, --batch=10                 The maximum number of messages to move at a time.
  -v, --version                  Show application version.
```

Examples:

Region will default to `us-west-2`, you can also override it with `--region` flag

```
sqsmover --source=my_source_queue_name --destination=my_destination_queuename
sqsmover --source=my_source_queue_name --destination=my_destination_queuename --region=eu-west-1

-- shorthand
sqsmover -s my_source_queue_name -d my_destination_queuename -r eu-west-1
```

Profile will default to `Default`, you can also override it with `--profile` flag

```
sqsmover --source=my_source_queue_name --destination=my_destination_queuename --profile=user
```

Limit number of moved messages to 10
```
sqsmover -s my_source_queue_name -d my_destination_queuename -l 10
```

By default, `sqsmover` will try to move 10 messages at a time. However, if the total size of messages
in a batch exceeds 256kb (262,144 bytes) you will receive an error: `Batch requests cannot be longer than 262144 bytes. You have sent x bytes.`
To resolve, reduce the batch size by setting `-b` flag.
```
sqsmover -s my_source_queue_name -d my_destination_queuename -b 3
```

## Compiling from source

You will need to have [Golang installed](https://golang.org/doc/install).

```sh
# clone it outside GOPATH
git clone https://github.com/mercury2269/sqsmover
cd sqsmover

# get dependencies using go modules (needs go 1.11+)
go get ./...

# build
go build -o sqsmover .

# check it works
./sqsmover --version
```
