# sqsmover
a tool to move messages between sqs queue.

Usage:  `sqsmover -from {src queue name} -to {dst queue name}`

Other optioal flags:
```
  -limit {number}         maximum number of messages to be moved, default 0: unlimited
  -parallel {number}      numers of messages move in parallel, default 10.
  -profile {aws profile}  default "default"
  -region {aws region}    default "us-west-2"
```
