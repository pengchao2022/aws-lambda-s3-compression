# AWS LAMBDA S3 Compression 

This tutorial will use the AWS lambda to compress the file uploaded in source s3 bucket , then delete the files after compressed , then move the compressed files to target s3 bucket .

## Features

- Compress the s3 bucket 
- Triggers in EventBridge to create rules rate(5 minutes) 
- Or cron(0 8,20 * * ? *) (two times in one day)

## Usage

- Setup variables:
```shell
SOURCE_BUCKET=lambdaneedstocompression909090
TARGET_BUCKET=lambdacompressedfiles8888 
SOURCE_PREFIX=cloudfiles/
MINUTES_BACK=5  # or HOURS_BACK=24
DELETE_ORIGINAL=true
MAX_FILES=1000

```
![lambda console pic](<variables.png>)

- Or you can first run deploy.sh to get the zip file then run aws-cli-deploy.sh for the whole process

- make sure the IAM role has the policy.json file associated

- create cloudwatch event or Eventbridge to add triggers
   - rate (5 minutes) (just for example)

- here's the outputs after lambda executed


![executed](<executed.png>)