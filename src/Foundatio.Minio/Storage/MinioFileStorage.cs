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
using Minio.DataModel.Args;
using Minio.Exceptions;

namespace Foundatio.Storage
{
    public class MinioFileStorage : IFileStorage
    {
        private readonly string _bucket;
        private readonly bool _shouldAutoCreateBucket;
        private bool _bucketExistsChecked;
        private readonly IMinioClient _client;
        private readonly ISerializer _serializer;
        private readonly ILogger _logger;

        public MinioFileStorage(MinioFileStorageOptions options)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            _serializer = options.Serializer ?? DefaultSerializer.Instance;
            _logger = options.LoggerFactory?.CreateLogger(GetType()) ?? NullLogger.Instance;

            (var client, string bucket) = CreateClient(options);
            _client = client;
            _bucket = bucket;
            _shouldAutoCreateBucket = options.AutoCreateBucket;
        }

        public MinioFileStorage(Builder<MinioFileStorageOptionsBuilder, MinioFileStorageOptions> builder)
            : this(builder(new MinioFileStorageOptionsBuilder()).Build()) { }

        ISerializer IHaveSerializer.Serializer => _serializer;
        public IMinioClient Client => _client;

        private async Task EnsureBucketExists()
        {
            if (!_shouldAutoCreateBucket || _bucketExistsChecked)
                return;

            _logger.LogTrace("Checking if bucket {Bucket} exists", _bucket);
            bool found = await _client.BucketExistsAsync(new BucketExistsArgs().WithBucket(_bucket)).AnyContext();
            if (!found)
            {
                _logger.LogInformation("Creating {Bucket}", _bucket);
                await _client.MakeBucketAsync(new MakeBucketArgs().WithBucket(_bucket)).AnyContext();
                _logger.LogInformation("Created {Bucket}", _bucket);
            }

            _bucketExistsChecked = true;
        }

        [Obsolete($"Use {nameof(GetFileStreamAsync)} with {nameof(FileAccess)} instead to define read or write behaviour of stream")]
        public Task<Stream> GetFileStreamAsync(string path, CancellationToken cancellationToken = default)
            => GetFileStreamAsync(path, StreamMode.Read, cancellationToken);

        public async Task<Stream> GetFileStreamAsync(string path, StreamMode streamMode, CancellationToken cancellationToken = default)
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            if (streamMode is StreamMode.Write)
                throw new NotSupportedException($"Stream mode {streamMode} is not supported.");

            await EnsureBucketExists().AnyContext();

            string normalizedPath = NormalizePath(path);
            _logger.LogTrace("Getting file stream for {Path}", normalizedPath);

            try
            {
                Stream result = new MemoryStream();
                await _client.GetObjectAsync(new GetObjectArgs().WithBucket(_bucket).WithObject(normalizedPath).WithCallbackStream(async (stream, _) => await stream.CopyToAsync(result).AnyContext()), cancellationToken).AnyContext();
                result.Seek(0, SeekOrigin.Begin);
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unable to get file stream for {Path}: {Message}", normalizedPath, ex.Message);
                return null;
            }
        }

        public async Task<FileSpec> GetFileInfoAsync(string path)
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            await EnsureBucketExists().AnyContext();

            string normalizedPath = NormalizePath(path);
            _logger.LogTrace("Getting file info for {Path}", normalizedPath);

            try
            {
                var metadata = await _client.StatObjectAsync(new StatObjectArgs().WithBucket(_bucket).WithObject(normalizedPath)).AnyContext();
                return new FileSpec
                {
                    Path = normalizedPath,
                    Size = metadata.Size,
                    Created = metadata.LastModified.ToUniversalTime(),
                    Modified = metadata.LastModified.ToUniversalTime()
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unable to get file info for {Path}: {Message}", normalizedPath, ex.Message);
                return null;
            }
        }

        public async Task<bool> ExistsAsync(string path)
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            await EnsureBucketExists().AnyContext();

            string normalizedPath = NormalizePath(path);
            _logger.LogTrace("Checking if {Path} exists", normalizedPath);

            try
            {
                return await _client.StatObjectAsync(new StatObjectArgs().WithBucket(_bucket).WithObject(normalizedPath)).AnyContext() != null;
            }
            catch (Exception ex) when (ex is ObjectNotFoundException or BucketNotFoundException)
            {
                _logger.LogDebug(ex, "Unable to check if {Path} exists: {Message}", normalizedPath, ex.Message);
                return false;
            }
        }

        public async Task<bool> SaveFileAsync(string path, Stream stream, CancellationToken cancellationToken = default)
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            await EnsureBucketExists().AnyContext();

            string normalizedPath = NormalizePath(path);
            _logger.LogTrace("Saving {Path}", normalizedPath);

            var seekableStream = stream.CanSeek ? stream : new MemoryStream();
            if (!stream.CanSeek)
            {
                await stream.CopyToAsync(seekableStream).AnyContext();
                seekableStream.Seek(0, SeekOrigin.Begin);
            }

            try
            {
                await _client.PutObjectAsync(new PutObjectArgs().WithBucket(_bucket).WithObject(normalizedPath).WithStreamData(seekableStream).WithObjectSize(seekableStream.Length - seekableStream.Position), cancellationToken).AnyContext();
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving {Path}: {Message}", normalizedPath, ex.Message);
                return false;
            }
            finally
            {
                if (!stream.CanSeek)
                    seekableStream.Dispose();
            }
        }

        public async Task<bool> RenameFileAsync(string path, string newPath, CancellationToken cancellationToken = default)
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            if (String.IsNullOrEmpty(newPath))
                throw new ArgumentNullException(nameof(newPath));

            await EnsureBucketExists().AnyContext();

            string normalizedPath = NormalizePath(path);
            string normalizedNewPath = NormalizePath(newPath);
            _logger.LogInformation("Renaming {Path} to {NewPath}", normalizedPath, normalizedNewPath);

            return await CopyFileAsync(normalizedPath, normalizedNewPath, cancellationToken).AnyContext() &&
                   await DeleteFileAsync(normalizedPath, cancellationToken).AnyContext();
        }

        public async Task<bool> CopyFileAsync(string path, string targetPath, CancellationToken cancellationToken = default)
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            if (String.IsNullOrEmpty(targetPath))
                throw new ArgumentNullException(nameof(targetPath));

            await EnsureBucketExists().AnyContext();

            string normalizedPath = NormalizePath(path);
            string normalizedTargetPath = NormalizePath(targetPath);
            _logger.LogInformation("Copying {Path} to {TargetPath}", normalizedPath, normalizedTargetPath);

            try
            {
                var copySourceArgs = new CopySourceObjectArgs().WithBucket(_bucket).WithObject(normalizedPath);

                await _client.CopyObjectAsync(new CopyObjectArgs()
                    .WithBucket(_bucket)
                    .WithObject(normalizedTargetPath)
                    .WithCopyObjectSource(copySourceArgs), cancellationToken).AnyContext();
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error copying {Path} to {TargetPath}: {Message}", normalizedPath, normalizedTargetPath, ex.Message);
                return false;
            }
        }

        public async Task<bool> DeleteFileAsync(string path, CancellationToken cancellationToken = default)
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            await EnsureBucketExists().AnyContext();

            string normalizedPath = NormalizePath(path);
            _logger.LogTrace("Deleting {Path}", normalizedPath);

            try
            {
                await _client.RemoveObjectAsync(new RemoveObjectArgs().WithBucket(_bucket).WithObject(normalizedPath), cancellationToken).AnyContext();
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unable to delete {Path}: {Message}", normalizedPath, ex.Message);
                return false;
            }
        }

        public async Task<int> DeleteFilesAsync(string searchPattern = null, CancellationToken cancellation = default)
        {
            await EnsureBucketExists().AnyContext();

            var files = await GetFileListAsync(searchPattern, cancellationToken: cancellation).AnyContext();
            _logger.LogInformation("Deleting {FileCount} files matching {SearchPattern}", files.Count, searchPattern);
            if (files.Count == 0)
            {
                _logger.LogTrace("Finished deleting {FileCount} files matching {SearchPattern}", files.Count, searchPattern);
                return 0;
            }

            var result = await _client.RemoveObjectsAsync(new RemoveObjectsArgs().WithBucket(_bucket).WithObjects(files.Select(spec => NormalizePath(spec.Path)).ToList()), cancellation).AnyContext();
            var resetEvent = new AutoResetEvent(false);
            result.Subscribe(error =>
            {
                _logger.LogError("Error deleting {Path}: {Message}", error.Key, error.Message);
                resetEvent.Set();
            }, () => resetEvent.Set());
            resetEvent.WaitOne();

            int count = await result.Count();
            _logger.LogTrace("Finished deleting {FileCount} files matching {SearchPattern}", count, searchPattern);
            return count;
        }

        public async Task<PagedFileListResult> GetPagedFileListAsync(int pageSize = 100, string searchPattern = null, CancellationToken cancellationToken = default)
        {
            if (pageSize <= 0)
                return PagedFileListResult.Empty;

            await EnsureBucketExists().AnyContext();

            var result = new PagedFileListResult(_ => GetFiles(searchPattern, 1, pageSize, cancellationToken));
            await result.NextPageAsync().AnyContext();
            return result;
        }

        private async Task<NextPageResult> GetFiles(string searchPattern, int page, int pageSize, CancellationToken cancellationToken)
        {
            int pagingLimit = pageSize;
            int skip = (page - 1) * pagingLimit;
            if (pagingLimit < Int32.MaxValue)
                pagingLimit++;

            var list = (await GetFileListAsync(searchPattern, pagingLimit, skip, cancellationToken).AnyContext()).ToList();
            bool hasMore = false;
            if (list.Count == pagingLimit)
            {
                hasMore = true;
                list.RemoveAt(pagingLimit - 1);
            }

            return new NextPageResult
            {
                Success = true,
                HasMore = hasMore,
                Files = list,
                NextPageFunc = hasMore ? _ => GetFiles(searchPattern, page + 1, pageSize, cancellationToken) : null
            };
        }

        private Task<List<FileSpec>> GetFileListAsync(string searchPattern = null, int? limit = null, int? skip = null, CancellationToken cancellationToken = default)
        {
            if (limit is <= 0)
                return Task.FromResult(new List<FileSpec>());

            var list = new List<Item>();
            var criteria = GetRequestCriteria(searchPattern);

            _logger.LogTrace(
                s => s.Property("SearchPattern", searchPattern).Property("Limit", limit).Property("Skip", skip),
                "Getting file list recursively matching {Prefix} and {Pattern}...", criteria.Prefix, criteria.Pattern
            );

            ExceptionDispatchInfo exception = null;
            var resetEvent = new AutoResetEvent(false);
            var observable = _client.ListObjectsAsync(new ListObjectsArgs().WithBucket(_bucket).WithPrefix(criteria.Prefix).WithRecursive(true), cancellationToken);
            observable.Subscribe(item =>
            {
                if (item.IsDir)
                    return;

                if (criteria.Pattern != null && !criteria.Pattern.IsMatch(item.Key))
                {
                    _logger.LogTrace("Skipping {Path}: Doesn't match pattern", item.Key);
                    return;
                }

                list.Add(item);
            }, error =>
            {
                if (error.GetType().ToString() != "Minio.EmptyBucketOperation")
                {
                    _logger.LogError(error, "Error getting file list: {Message}", error.Message);
                    exception = ExceptionDispatchInfo.Capture(error);
                }
                resetEvent.Set();
            },
                () => resetEvent.Set()
            );
            resetEvent.WaitOne();
            exception?.Throw();

            if (skip.HasValue)
                list = list.Skip(skip.Value).ToList();

            if (limit.HasValue)
                list = list.Take(limit.Value).ToList();

            return Task.FromResult(list.Select(blob => new FileSpec
            {
                Path = blob.Key,
                Size = (long)blob.Size,
                Modified = DateTime.Parse(blob.LastModified),
                Created = DateTime.Parse(blob.LastModified)
            }).ToList());
        }

        private string NormalizePath(string path)
        {
            return path?.Replace('\\', '/');
        }

        private class SearchCriteria
        {
            public string Prefix { get; set; }
            public Regex Pattern { get; set; }
        }

        private SearchCriteria GetRequestCriteria(string searchPattern)
        {
            if (String.IsNullOrEmpty(searchPattern))
                return new SearchCriteria { Prefix = String.Empty };

            string normalizedSearchPattern = NormalizePath(searchPattern);
            int wildcardPos = normalizedSearchPattern.IndexOf('*');
            bool hasWildcard = wildcardPos >= 0;

            string prefix = normalizedSearchPattern;
            Regex patternRegex = null;

            if (hasWildcard)
            {
                patternRegex = new Regex($"^{Regex.Escape(normalizedSearchPattern).Replace("\\*", ".*?")}$");
                int slashPos = normalizedSearchPattern.LastIndexOf('/');
                prefix = slashPos >= 0 ? normalizedSearchPattern.Substring(0, slashPos) : String.Empty;
            }

            return new SearchCriteria
            {
                Prefix = prefix,
                Pattern = patternRegex
            };
        }

        private (IMinioClient Client, string Bucket) CreateClient(MinioFileStorageOptions options)
        {
            var connectionString = new MinioFileStorageConnectionStringBuilder(options.ConnectionString);

            string endpoint;
            bool secure;
            if (connectionString.EndPoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            {
                endpoint = connectionString.EndPoint.Substring(8);
                secure = true;
            }
            else
            {
                endpoint = connectionString.EndPoint.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
                    ? connectionString.EndPoint.Substring(7)
                    : connectionString.EndPoint;
                secure = false;
            }

            var client = new MinioClient()
                .WithEndpoint(endpoint)
                .WithCredentials(connectionString.AccessKey, connectionString.SecretKey);

            if (!String.IsNullOrEmpty(connectionString.Region))
                client.WithRegion(connectionString.Region ?? String.Empty);

            client.Build();

            if (secure)
                client.WithSSL();

            return (client, connectionString.Bucket);
        }

        public void Dispose() { }
    }
}
