# S3Sync

#### 这是最快速且最具成本效益的 S3 同步工具。

## 特性
* 使用 S3 Copy API 复制文件
* 支持多线程复制
* 支持使用本地密钥文件进行复制，以减少 S3 列表成本
* S3 Copy API 的速率限制
* 支持前缀
* 支持记录文件状态（如同步成功），以防止重复执行 S3Copy
* 支持 S3 分片上传（使用并行上传），如果文件大于 5GB，s3Copy 将会失败
* 支持失败重试（使用指数退避重试）

## 待办事项


## 为什么更快更具成本效益？
* 使用 S3 Copy API 复制文件，无需下载和上传文件，无需关心网络带宽。

* 使用本地Key文件进行复制以减少 S3 列表成本，当您的存储桶文件数量较大时（如 1 亿个文件），可以启用 s3 存储清单配置来获取所有文件列表。

## 限制
* S3 API 请求的速率限制为每秒 3500 个请求（每个前缀）。
  因此我们的请求速率不能超过 3500,内置了2000qps的限流器！

## 使用 S3 Inventory 配置获取所有文件列表
S3 Inventory 是一个允许您获取存储桶中所有文件列表的功能，启用后需要等待 48 小时才能获取所有文件列表。
如果我们获取到这个，就不需要使用 S3List API 来获取所有文件列表。S3 Inventory 比 S3List API 更便宜。

## 记录
将记录文件复制状态，如果文件已成功复制，则不会再次复制。当您再次运行同步时，它将跳过已复制的文件。

## Inventory 配置文件列表拆分
如果文件列表很大，比如 1 亿个文件，我们需要将文件列表拆分成多个文件，默认每个文件 5000000 条记录。

### 如何使用？
在 S3 路径：s3://aispace-inventory/you-bucket-name/you-bucket-name/all-inventory/ 中，您将找到以下结构：

```
├── 2024-12-20T01-00Z/
│   ├── manifest.json
│   └── manifest.checksum
├── data/
│   ├── 123456789.csv.gz
│   └── ...
└── hive/
```
所有文件列表都在 data/ 中，manifest.json 是文件列表的快照，就像索引：
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
使用这个 manifest.json，我们不需要解析 /data 目录中的所有 csv.gz

## 使用方法
```
% ./sync -h
\在 S3 存储桶之间同步文件/S3

用法：
  sync [flags]

参数：
  -b, --bind-port int                    服务器端口（默认 8080）
  -h, --help                             帮助
      --inventory-bucket string          输入存储桶
      --inventory-file-batch-size int    存储清单文件批次大小（默认 5000000）
      --inventory-manifest-file string   存储清单清单文件（默认 "manifest.json"）
      --inventory-output-file string     输出文件
      --inventory-prefix string          存储清单配置前缀
  -m, --mode string                      模式（默认 "sync"）
      --pprof                            pprof
  -p, --prefix string                    前缀
  -f, --record-file string              记录文件
  -r, --region string                    区域（默认 "us-east-1"）
  -s, --source-bucket string            源存储桶（默认 "null"）
  -t, --target-bucket string            目标存储桶（默认 "null"）
  -v, --version                         显示版本
  -w, --workers int                     工作线程数（默认 10）
```

# 示例
* 不使用清单同步 S3 存储桶到另一个 S3 存储桶
```
# sync -s s3://source-bucket -t s3://target-bucket -w 10 -r us-east-1 
```
* 使用本地密钥文件同步 S3 存储桶到另一个 S3 存储桶
```
# sync -s s3://source-bucket -t s3://target-bucket -w 10 -r us-east-1 -f record.txt
```
* 从 S3 Inventory 获取文件列表
```
# ./sync -m preprocess --inventory-bucket you-inventory-bucket-name --inventory-output-file keys.txt --inventory-prefix you-bucket-name/you-bucket-name/all-inventory/data/
```

## 安装
```
# cd cmd/sync
# go build -o sync .
```

## 贡献

## 许可证
GPLv3