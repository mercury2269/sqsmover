## Installing
go get -u github.com/mercury2269/sqsmover/...

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

```
sqsmover -source=mysourcequeuename -dest=mydestinationqueuename
```


