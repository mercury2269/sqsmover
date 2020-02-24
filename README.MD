# SQS Message Mover
SQS Mover moves messages from one AWS SQS queue to another. Useful when you need
to move deadletter queue messages back into the original queue.

## Features

* Reliable delivery. Messages are only deleted from the source queue if they were enqueued to the destination.
* Messages are sent and received in batches for faster processing.
* Progress indicator.
* Friendly output and error message.
* Queue name resolution. For ease of use, you only need to provide a queue name and not the full `arn` address.
* Message Attributes are copied over.
* Support for FIFO queues. MessageGroupId and MessageDeduplicationId are copied over to the destination messages.
* Optional flag to limit the number of messages to move.

## Installation

SQS Mover works on most operating systems (macOS, Linux, Windows) without additional dependencies. You can install
the pre-compiled binary (in several different ways) or compile from source.

### Install the pre-compiled binary

**homebrew tap** (only on macOS for now):
```
brew install mercury2269/homebrew-tap/sqsmover
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

**manually (including Windows)**:

Download the pre-compiled binaries from the [releases page](https://github.com/mercury2269/sqsmover/releases) and
copy to the desired location.

### Compiling from source

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

## Configuring Credentials
Before using SQS Mover ensure that you've configured AWS credentials. The best
way to configure credentials on a development machine is to create the credentials file
`~/.aws/credentials`, which might look like:

```
[default]
aws_access_key_id = <YOUR_ACCESS_KEY_ID>
aws_secret_access_key = <YOUR_SECRET_ACCESS_KEY>
```

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

```
sqsmover --help

usage: sqsmover.exe --source=SOURCE --destination=DESTINATION [<flags>]

Flags:
  -h, --help                     Show context-sensitive help (also try
                                 --help-long and --help-man).
  -s, --source=SOURCE            Source queue name to move messages from.
  -d, --destination=DESTINATION  Destination queue name to move messages to.
  -r, --region="us-west-2"       AWS region for source and destination queues.
  -p, --profile=""               Use a specific profile from AWS credentials file.
  -l, --limit=0                  Limits number of messages moved. No limit is set by default.
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

