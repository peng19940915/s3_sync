# S3Sync

> English | [中文](readme_zh.md)

#### This is the fastest and most cost-effective S3 synchronization tool.

## Features
* Use S3 Copy API to copy files.
* Support multi-threaded copy.
* Supports using local key files for replication to reduce S3 list costs.
* Rate limit for S3 Copy API.
* Supports Prefix
* Supports recording file status, such as successfully synchronized, to prevent redundant execution of S3Copy.
* Support S3 Multipart Upload(use parallel upload),if the file is large than 5GB, s3Copy will be failed.
* Support failed copy retry(use backoff retry with exponential)

## TODO


## Why is it faster and more cost-effective?
* Use S3 Copy API to copy files, don't need to download and upload files, don't need to care about the network bandwidth. s3Copy speed can reach 5Gbps(single file).

* Use local key files for replication to reduce S3 list costs, you can enable the s3 storage inventory configuration to get all file list when you bucket's file count is large, such as 100 million files.

## Limitation
* The S3 API request has a rate limit of 3500 requests per second(each prefix).
  So our request rate cannot exceed 3500

## Use S3 Inventory configurations to get all file list
S3 Inventory is a feature that allows you to get all file list in a bucket, if enable this need wait 48 hours to get all file list. 
if we get this, we don't need to use S3List API to get all file list. S3 Inventory is more cheaper than S3List API.

## records
will record the file copy status, if the file is successfully copied, it will not be copied again,when you run sync again, it will skip the file that has been copied.

## Inventory Configuration file list split
if the file list is large, such as 100 million files, we need to split the file list into multiple files, 5000000 files per file.
## Compare with other solutions
### AWS Data Sync

### AWS S3 Replication Copy（US-East-1）
- SRR
API Request Costs
   - Source Bucket GET: 1000 × ($0.0004/1000) = $0.0004
   - Source Bucket HEAD: 1000 × ($0.0004/1000) = $0.0004
   - LIST Requests: ~10 × ($0.0005/1000) = $0.000005
   
   - Destination Bucket PUT: 1000 × ($0.005/1000) = $0.005
   - Destination Bucket HEAD: 1000 × ($0.0004/1000) = $0.0004
Total: $0.005805
- CRR

### How to use?
In S3 Path: s3://aispace-inventory/you-bucket-name/you-bucket-name/all-inventory/ you will find the following structure:

```
├── 2024-12-20T01-00Z/
│   ├── manifest.json
│   └── manifest.checksum
├── data/
│   ├── 123456789.csv.gz
│   └── ...
└── hive/
```
all file list is in data/, the manifest.json is the snapshot of the file list, just like index:
```json
{
  "sourceBucket" : "blazers-aigc",
  "destinationBucket" : "arn:aws:s3:::aispace-inventory",
  "version" : "2016-11-30",
  "creationTimestamp" : "1735002000000",
  "fileFormat" : "CSV",
  "fileSchema" : "Bucket, Key, Size, LastModifiedDate",
  "files" : [ {
    "key" : "you-bucket-name/you-bucket-name/all-inventory/data/7dfc0bdc-f886-473e-956b-99d1ea278f89.csv.gz",
    "size" : 21049370,
    "MD5checksum" : "403576ada95ac133c34c810aa0a2e6ee"
  }
}
```
use this manifest.json we dont't need to parse all csv.gz in directory: /data
## Usage
```
% ./sync -h
\Sync files/S3 between S3 buckets

Usage:
  sync [flags]

Flags:
  -b, --bind-port int                    server port (default 8080)
  -h, --help                             help for sync
      --inventory-bucket string          Input Bucket
      --inventory-file-batch-size int    Storage inventory file batch size (default 5000000)
      --inventory-manifest-file string   Storage inventory manifest file (default "manifest.json")
      --inventory-output-file string     Output file
      --inventory-prefix string          Storage inventory configuration prefix
  -m, --mode string                      Mode (default "sync")
      --pprof                            pprof
  -p, --prefix string                    Prefix
  -f, --record-file string               Record file
  -r, --region string                    Region (default "us-east-1")
  -s, --source-bucket string             Source bucket (default "null")
  -t, --target-bucket string             Target bucket (default "null")
  -v, --version                          Show version
  -w, --workers int                      Workers (default 10)

```

# Example
* Sync S3 bucket to Another S3 bucket without inventory
```
# sync -s s3://source-bucket -t s3://target-bucket -w 10 -r us-east-1 
```
* Sync S3 bucket to another S3 bucket use local key files
```
# sync -s s3://source-bucket -t s3://target-bucket -w 10 -r us-east-1 -f record.txt
```
* Get file list from S3 Inventory
```
# ./sync -m preprocess --inventory-bucket you-inventory-bucket-name --inventory-output-file keys.txt --inventory-prefix you-bucket-name/you-bucket-name/all-inventory/data/
```

## Install
```
# cd cmd/sync
# go build -o sync .
```

## Contributing

## License
GPLv3