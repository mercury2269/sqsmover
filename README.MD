# SQS Message Mover
A simple and rough application that moves messages from one AWS SQS queue to 
another. Useful when you need to move deadletter queue messages back into the 
source queue. It hasn't been thoroughly tested, please **use at your own risk** or modify
as needed. 


## Installing

Download and intall Go https://golang.org/dl/

Install binary with dependencies using the following command:

```
go get -u github.com/mercury2269/sqsmover/...
```

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
sqsmover -source=my_source_queue_name -dest=my_destination_queuename
sqsmover -source=my_source_queue_name -dest=my_destination_queuename -region=eu-west-1
```



