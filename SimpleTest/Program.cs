/*
 * MinIO .NET Library for Amazon S3 Compatible Cloud Storage, (C) 2017 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Net;
using System.Reactive.Linq;
using Minio;
using Minio.DataModel.Args;

namespace SimpleTest;

public static class Program
{
    private static async Task Main()
    {
        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12
                                               | SecurityProtocolType.Tls11
                                               | SecurityProtocolType.Tls12;

        // Note: s3 AccessKey and SecretKey needs to be added in App.config file
        // See instructions in README.md on running examples for more information.
        using var minio = new MinioClient()
            .WithEndpoint("localhost:9000")
            .WithCredentials("admin",
                "admin123").WithSSL(false)
            .Build();
        var testBucketName = "marshall-back";
        var fileName = "123.jpg";
        var nmuArgs = new NewMultipartUploadPutArgs()
            .WithBucket(testBucketName)
            .WithContentType("application/octet-stream")
            .WithObject(fileName);
        var uploadId = await minio.NewMultipartUploadAsync(nmuArgs).ConfigureAwait(false);
        //var uploadId = "MTExZDRlZTUtM2UwMC00MjI3LTg0YmUtN2YyMWUyMDgzYzg5LjFhYWU2ZWQ2LTQyZWItNDg5ZS1hMTU4LTA1OTdlN2NiYzgzYw";
        Console.WriteLine($"uploadId:{uploadId}");


        var filePath = @"C:\123.jpg"; // 图片文件路径
        var chunkSize = 1024 * 1024 * 5; // 分片大小，这里设置为1MB
        var partNumber = 1; // 分片索引初始化
        var etags = new Dictionary<int, string>();
        //etags.Add(1,"\"bd60b2de60551dfa3e23553611409930\"");
        using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
        {
            int bytesRead;
            var buffer = new byte[chunkSize];

            while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false)) > 0)
            {
                // 如果实际读取的字节小于分片大小，调整缓冲区大小以匹配实际读取的字节
                if (bytesRead < chunkSize)
                {
                    var actualBuffer = new byte[bytesRead];
                    Array.Copy(buffer, actualBuffer, bytesRead);
                    buffer = actualBuffer;
                }

                //if (partNumber == 2)
                //{
                //    break;
                //}

                //if (partNumber != 1)
                //{
                    using var memoryStream = new MemoryStream(buffer);
                    // 调用分片上传方法
                    var etag = await UploadChunk(minio, uploadId, testBucketName,
                        fileName, partNumber, bytesRead, memoryStream).ConfigureAwait(false);
                    etags.Add(partNumber, etag);

                    Console.WriteLine($"Chunk:{partNumber},{etag}");
                //}

                partNumber++; // 为下一个分片增加索引
            }
        }
        var completeMultipartUploadArgs = new CompleteMultipartUploadArgs()
            .WithBucket(testBucketName)
            .WithObject(fileName)
            .WithUploadId(uploadId)
            .WithETags(etags);
        var r = await minio.CompleteMultipartUploadAsync(completeMultipartUploadArgs).ConfigureAwait(false);
        //Console.WriteLine($"Completed:{r.ObjectName}");
        //await minio.RemoveIncompleteUploadAsync(new RemoveIncompleteUploadArgs().WithBucket(testBucketName)
        //    .WithObject(fileName)).ConfigureAwait(false);


    }
    private static async Task<string> UploadChunk(IMinioClient minio,string uploadId,string bucket,string fileName, int partNumber, int chunkSize, MemoryStream chunkStream)
    {
        var putObjectPartArgs = new PutObjectArgs()
            .WithBucket(bucket)
            .WithObject(fileName)
            .WithObjectSize(chunkSize)
            .WithUploadId(uploadId)
            .WithPartNumber(partNumber)
            .WithStreamData(chunkStream);
        var resp = await minio.PutObjectPartAsync(putObjectPartArgs).ConfigureAwait(false);
        return resp.Etag;
    }

    private static Task<bool> IsBucketExists(IMinioClient minio, string bucketName)
    {
        var bktExistsArgs = new BucketExistsArgs().WithBucket(bucketName);
        return minio.BucketExistsAsync(bktExistsArgs);
    }
}
