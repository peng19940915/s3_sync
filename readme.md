# S3Sync

#### This is the fastest and most cost-effective S3 synchronization tool.

## Features
* Use S3 Copy API to copy files.
* Support multi-threaded copy.
* Supports using local key files for replication to reduce S3 list costs.
* Rate limit for S3 Copy API.
* Supports Prefix
* Supports recording file status, such as successfully synchronized, to prevent redundant execution of S3Copy. 

## TODO
* Support S3 Multipart Upload,if the file is large than 5GB, s3Copy will be failed.

## Why is it faster and more cost-effective?
* Use S3 Copy API to copy files, don't need to download and upload files, don't need to care about the network bandwidth. s3Copy speed can reach 5Gbps(single file).
* Use local key files for replication to reduce S3 list costs, you can enable the s3 storage lens to get all file list when you bucket's file count is large, such as 100 million files.
## Limitation
* The S3 Copy API has a rate limit of 500 requests per second.
* The S3 Copy API has a rate limit of 500 requests per second.

## DuckDB
* https://github.com/marcboeker/go-duckdb?tab=readme-ov-file#linking-duckdb

## Usage
```
# sync -h
Sync files/S3 between S3 buckets

Usage:
  sync [flags]

Flags:
  -h, --help                   help for sync
  -p, --prefix string          Prefix
  -f, --record-file string     Record file
  -r, --region string          Region (default "us-east-1")
  -s, --source-bucket string   Source bucket (default "null")
  -t, --target-bucket string   Target bucket (default "null")
  -v, --version                Show version
  -w, --workers int            Workers (default 10)

```

# Example
* Sync S3 bucket to Another S3 bucket
```
# sync -s s3://source-bucket -t s3://target-bucket -w 10 -r us-east-1 
```
* Sync S3 bucket to another S3 bucket use local key files
```
# sync -s s3://source-bucket -t s3://target-bucket -w 10 -r us-east-1 -f record.txt
```

## Install
```
# cd cmd/sync
# go build -o sync .
```

## Contributing

## License
GPLv3