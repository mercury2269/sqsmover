# sqsmover
a tool to move messages between sqs queue.

Usage:  `sqsmover -source {src queue name} -destination {dst queue name}`

Other optioal flags:
```
  -limit {number}         Limits total number of messages moved. No limit is set by default.
  -parallel {number}      Max numers of messages to be moved in parallel (default 10).
  -profile {aws profile}  Use a specific profile from AWS credentials file (default "default").
  -region {aws region}    The AWS region for source and destination queues (default "us-west-2").
```
