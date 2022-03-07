using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Runtime.ExceptionServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Extensions;
using Foundatio.Serializer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Minio;
using Minio.DataModel;
using Minio.Exceptions;

namespace Foundatio.Storage {
    public class MinioFileStorage : IFileStorage {
        private readonly string _bucket;
        private readonly bool _shouldAutoCreateBucket = false;
        private bool _bucketExistsChecked = false;
        private readonly MinioClient _client;
        private readonly ISerializer _serializer;
        private readonly ILogger _logger;

        public MinioFileStorage(MinioFileStorageOptions options) {
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            var connectionString = new MinioFileStorageConnectionStringBuilder(options.ConnectionString);
            string endpoint;
            bool secure;
            if (connectionString.EndPoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase)) {
                endpoint = connectionString.EndPoint.Substring(8);
                secure = true;
            } else {
                secure = false;
                endpoint = connectionString.EndPoint.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
                    ? connectionString.EndPoint.Substring(7)
                    : connectionString.EndPoint;
            }

            _client = new MinioClient()
                .WithEndpoint(endpoint)
                .WithCredentials(connectionString.AccessKey, connectionString.SecretKey);
            
            if (!String.IsNullOrEmpty(connectionString.Region))
                _client.WithRegion(connectionString.Region ?? String.Empty);

            _client.Build();

            if (secure)
                _client.WithSSL();
            
            _bucket = connectionString.Bucket;
            _shouldAutoCreateBucket = options.AutoCreateBucket;
            _serializer = options.Serializer ?? DefaultSerializer.Instance;
            _logger = options.LoggerFactory?.CreateLogger(typeof(MinioFileStorage)) ?? NullLogger.Instance;
        }

        public MinioFileStorage(Builder<MinioFileStorageOptionsBuilder, MinioFileStorageOptions> builder)
            : this(builder(new MinioFileStorageOptionsBuilder()).Build()) { }

        ISerializer IHaveSerializer.Serializer => _serializer;

        private async Task EnsureBucketExists() {
            if (!_shouldAutoCreateBucket || _bucketExistsChecked)
                return;
            
            bool found = await _client.BucketExistsAsync(new BucketExistsArgs().WithBucket(_bucket));
            if (!found)
                await _client.MakeBucketAsync(new MakeBucketArgs().WithBucket(_bucket));
            
            _bucketExistsChecked = true;
        }

        public async Task<Stream> GetFileStreamAsync(string path, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            await EnsureBucketExists().AnyContext();

            try {
                Stream result = new MemoryStream();
                await _client.GetObjectAsync(new GetObjectArgs().WithBucket(_bucket).WithObject(NormalizePath(path)).WithCallbackStream(stream => stream.CopyToAsync(result).GetAwaiter().GetResult()), cancellationToken).AnyContext();
                result.Seek(0, SeekOrigin.Begin);
                return result;
            } catch (Exception ex) {
                _logger.LogTrace(ex, "Error trying to get file stream: {Path}", path);
                return null;
            }
        }

        public async Task<FileSpec> GetFileInfoAsync(string path) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            await EnsureBucketExists().AnyContext();

            path = NormalizePath(path);
            try {
                var metadata = await _client.StatObjectAsync(new StatObjectArgs().WithBucket(_bucket).WithObject(path)).AnyContext();
                return new FileSpec {
                    Path = path,
                    Size = metadata.Size,
                    Created = metadata.LastModified.ToUniversalTime(),
                    Modified = metadata.LastModified.ToUniversalTime()
                };
            } catch (Exception) {
                return null;
            }
        }

        public async Task<bool> ExistsAsync(string path) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            await EnsureBucketExists().AnyContext();

            try {
                return await _client.StatObjectAsync(new StatObjectArgs().WithBucket(_bucket).WithObject(NormalizePath(path))).AnyContext() != null;
            } catch (Exception ex) when(ex is ObjectNotFoundException || ex is BucketNotFoundException) {
                return false;
            }
        }

        public async Task<bool> SaveFileAsync(string path, Stream stream, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            await EnsureBucketExists().AnyContext();

            var seekableStream = stream.CanSeek ? stream : new MemoryStream();
            if (!stream.CanSeek) {
                await stream.CopyToAsync(seekableStream).AnyContext();
                seekableStream.Seek(0, SeekOrigin.Begin);
            }

            try {
                await _client.PutObjectAsync(new PutObjectArgs().WithBucket(_bucket).WithObject(NormalizePath(path)).WithStreamData(seekableStream).WithObjectSize(seekableStream.Length - seekableStream.Position), cancellationToken).AnyContext();
                return true;
            } catch (Exception ex) {
                _logger.LogError(ex, "Error trying to save file: {Path}", path);
                return false;
            } finally {
                if (!stream.CanSeek)
                    seekableStream.Dispose();
            }
        }

        public async Task<bool> RenameFileAsync(string path, string newPath, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            if (String.IsNullOrEmpty(newPath))
                throw new ArgumentNullException(nameof(newPath));

            await EnsureBucketExists().AnyContext();

            path = NormalizePath(path);
            newPath = NormalizePath(newPath);
            return await CopyFileAsync(path, newPath, cancellationToken).AnyContext() &&
                   await DeleteFileAsync(path, cancellationToken).AnyContext();
        }

        public async Task<bool> CopyFileAsync(string path, string targetPath, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            if (String.IsNullOrEmpty(targetPath))
                throw new ArgumentNullException(nameof(targetPath));

            await EnsureBucketExists().AnyContext();

            try {
                var copySourceArgs = new CopySourceObjectArgs().WithBucket(_bucket).WithObject(NormalizePath(path));
                
                await _client.CopyObjectAsync(new CopyObjectArgs()
                    .WithBucket(_bucket)
                    .WithObject(NormalizePath(targetPath))
                    .WithCopyObjectSource(copySourceArgs), cancellationToken).AnyContext();
                return true;
            } catch (Exception ex) {
                _logger.LogError(ex, "Error trying to copy file {Path} to {TargetPath}.", path, targetPath);
                return false;
            }
        }

        public async Task<bool> DeleteFileAsync(string path, CancellationToken cancellationToken = default) {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            await EnsureBucketExists().AnyContext();

            try {
                await _client.RemoveObjectAsync(new RemoveObjectArgs().WithBucket(_bucket).WithObject(NormalizePath(path)), cancellationToken).AnyContext();
                return true;
            } catch (Exception ex) {
                _logger.LogDebug(ex, "Error trying to delete file: {Path}.", path);
                return false;
            }
        }

        public async Task<int> DeleteFilesAsync(string searchPattern = null, CancellationToken cancellation = default) {
            await EnsureBucketExists().AnyContext();

            var files = await GetFileListAsync(searchPattern, cancellationToken: cancellation).AnyContext();
            if (files.Count() == 0)
                return 0;

            var result = await _client.RemoveObjectsAsync(new RemoveObjectsArgs().WithBucket(_bucket).WithObjects(files.Select(spec => NormalizePath(spec.Path)).ToList()), cancellation).AnyContext();
            var resetEvent = new AutoResetEvent(false);
            result.Subscribe(error => {
                _logger.LogError("Error trying to delete file {FilePath}: {Message}", error.Key, error.Message);
                resetEvent.Set();
            }, ()=> resetEvent.Set());
            resetEvent.WaitOne();

            return await result.Count();
        }

        public async Task<PagedFileListResult> GetPagedFileListAsync(int pageSize = 100, string searchPattern = null, CancellationToken cancellationToken = default) {
            if (pageSize <= 0)
                return PagedFileListResult.Empty;

            await EnsureBucketExists().AnyContext();

            searchPattern = NormalizePath(searchPattern);

            var result = new PagedFileListResult(r => GetFiles(searchPattern, 1, pageSize, cancellationToken));
            await result.NextPageAsync().AnyContext();
            return result;
        }

        private async Task<NextPageResult> GetFiles(string searchPattern, int page, int pageSize, CancellationToken cancellationToken) {
            int pagingLimit = pageSize;
            int skip = (page - 1) * pagingLimit;
            if (pagingLimit < Int32.MaxValue)
                pagingLimit++;

            var list = (await GetFileListAsync(searchPattern, pagingLimit, skip, cancellationToken).AnyContext()).ToList();
            bool hasMore = false;
            if (list.Count == pagingLimit) {
                hasMore = true;
                list.RemoveAt(pagingLimit - 1);
            }

            return new NextPageResult {
                Success = true,
                HasMore = hasMore,
                Files = list,
                NextPageFunc = hasMore ? r => GetFiles(searchPattern, page + 1, pageSize, cancellationToken) : (Func<PagedFileListResult, Task<NextPageResult>>)null
            };
        }

        private Task<IEnumerable<FileSpec>> GetFileListAsync(string searchPattern = null, int? limit = null, int? skip = null, CancellationToken cancellationToken = default) {
            if (limit.HasValue && limit.Value <= 0)
                return Task.FromResult(Enumerable.Empty<FileSpec>());

            var criteria = GetRequestCriteria(NormalizePath(searchPattern));

            var objects = new List<Item>();
            ExceptionDispatchInfo exception = null;
            var resetEvent = new AutoResetEvent(false);
            var observable = _client.ListObjectsAsync(new ListObjectsArgs().WithBucket(_bucket).WithPrefix(criteria.Prefix).WithRecursive(true), cancellationToken);
            observable.Subscribe(item => {
                    if (!item.IsDir && (criteria.Pattern == null || criteria.Pattern.IsMatch(item.Key))) {
                        objects.Add(item);
                    }
                }, error => {
                    if (error.GetType().ToString() != "Minio.EmptyBucketOperation") {
                        _logger.LogError(error, "Error trying to find files: {Pattern}", searchPattern);
                        exception = ExceptionDispatchInfo.Capture(error);
                    }
                    resetEvent.Set();
                },
                () => resetEvent.Set()
            );
            resetEvent.WaitOne();
            if (exception != null) {
                if (exception.SourceException is ObjectNotFoundException ||
                    exception.SourceException is BucketNotFoundException) {
                    return Task.FromResult(Enumerable.Empty<FileSpec>());
                }
                exception.Throw();
            }

            if (skip.HasValue)
                objects = objects.Skip(skip.Value).ToList();
            if (limit.HasValue)
                objects = objects.Take(limit.Value).ToList();

            return Task.FromResult(objects.Select(blob => new FileSpec {
                Path = blob.Key,
                Size = (long)blob.Size,
                Modified = DateTime.Parse(blob.LastModified),
                Created = DateTime.Parse(blob.LastModified)
            }));
        }

        private string NormalizePath(string path) {
            return path?.Replace('\\', '/');
        }

        private class SearchCriteria {
            public string Prefix { get; set; }
            public Regex Pattern { get; set; }
        }

        private SearchCriteria GetRequestCriteria(string searchPattern) {
            Regex patternRegex = null;
            searchPattern = searchPattern?.Replace('\\', '/');

            string prefix = searchPattern;
            int wildcardPos = searchPattern?.IndexOf('*') ?? -1;
            if (searchPattern != null && wildcardPos >= 0) {
                patternRegex = new Regex("^" + Regex.Escape(searchPattern).Replace("\\*", ".*?") + "$");
                int slashPos = searchPattern.LastIndexOf('/');
                prefix = slashPos >= 0 ? searchPattern.Substring(0, slashPos) : String.Empty;
            }

            return new SearchCriteria {
                Prefix = prefix ?? String.Empty,
                Pattern = patternRegex
            };
        }

        public void Dispose() { }
    }
}
