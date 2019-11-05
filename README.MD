# SQS Message Mover
SQS Mover lets you move messages from one AWS SQS queue to another. Useful when you need
to move deadletter queue messages back into the original queue.

## Features

* Reliable delivery. Messages are only deleted from the original queue if they 
were successfully un-queued to the destination.
* Messages are sent and received in batches for faster processing.
* Progress indicator.
* Queue name resolution. For ease of use, you only need to provide a queue name and not the full `arn` address. 

## Installation

On macOS, Linux, or OpenBSD run the following:

```
curl https://raw.githubusercontent.com/mercury2269/sqsmover/master/install.sh | sh
```

Note that you may need to run the `sudo` version below, or alternatively chown `/usr/local`:
```
curl https://raw.githubusercontent.com/mercury2269/sqsmover/master/install.sh | sudo sh
```

On Windows download [binary](https://github.com/mercury2269/sqsmover/releases).

After downloading, rename binary file 'sqsmover.exe', then add to PATH.

## Configuring Credentials
Before using the sqsmover ensure that you've configured AWS credentials. The best
way to configure credentials on a development machine is to create the credentials file
`~/.aws/credentials`, which might look like:

```
[default]
aws_access_key_id = <YOUR_ACCESS_KEY_ID>
aws_secret_access_key = <YOUR_SECRET_ACCESS_KEY>
```

As an alternative you can set AWS credentials in the environment variables.

The following examples show how you configure the environment variables.

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
      --help                     Show context-sensitive help (also try
                                 --help-long and --help-man).
  -s, --source=SOURCE            Source queue to move messages from
  -d, --destination=DESTINATION  Destination queue to move messages to
  -r, --region="us-west-2"       AWS Region for source and destination queues
  -p, --profile=""               Use a specific profile from your credential file.
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

