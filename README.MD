# SQS Message Mover
SQS Mover lets you move messages from one AWS SQS queue to another. Useful when you need
to move deadletter queue messages back into the original queue.


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
Before using the sqsmover ensure that you've configured credentials. The best
way to configure credentials on a development machine is to use the
`~/.aws/credentials` file, which might look like:

```
[default]
aws_access_key_id = AKID1234567890
aws_secret_access_key = MY-SECRET-KEY
```

## Usage

Supply the queue source and destination names as parameters as follows
Region will default to us-west-2, you can also override it with -region flag

```
sqsmover --source=my_source_queue_name --destination=my_destination_queuename
sqsmover --source=my_source_queue_name --destination=my_destination_queuename --region=eu-west-1
```



