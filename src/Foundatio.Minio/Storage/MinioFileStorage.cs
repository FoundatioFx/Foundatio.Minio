using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

namespace Foundatio.Storage;

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
            if (metadata.ExtraHeaders.TryGetValue("X-Minio-Error-Code", out string errorCode) && (String.Equals(errorCode, "NoSuchBucket") || String.Equals(errorCode, "NoSuchKey")))
                return null;

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
            var metadata = await _client.StatObjectAsync(new StatObjectArgs().WithBucket(_bucket).WithObject(normalizedPath)).AnyContext();
            if (metadata.ExtraHeaders.TryGetValue("X-Minio-Error-Code", out string errorCode) && (String.Equals(errorCode, "NoSuchBucket") || String.Equals(errorCode, "NoSuchKey")))
                return false;

            return true;
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

        _logger.LogInformation("Deleting files matching {SearchPattern}", searchPattern);

        int count = 0;
        var result = await GetPagedFileListAsync(250, searchPattern, cancellation).AnyContext();
        do
        {
            if (result.Files.Count == 0)
                break;

            var args = new RemoveObjectsArgs().WithBucket(_bucket)
                .WithObjects(result.Files.Select(spec => NormalizePath(spec.Path)).ToList());

            var response = await _client.RemoveObjectsAsync(args, cancellation).AnyContext();
            count += result.Files.Count;
            foreach (var error in response)
            {
                count--;
                _logger.LogError("Error deleting {Path}: {Message}", error.Key, error.Message);
            }
        } while (await result.NextPageAsync().AnyContext());

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

    private async Task<List<FileSpec>> GetFileListAsync(string searchPattern = null, int? limit = null, int? skip = null, CancellationToken cancellationToken = default)
    {
        if (limit is <= 0)
            return new List<FileSpec>();

        var list = new List<Item>();
        var criteria = GetRequestCriteria(searchPattern);

        _logger.LogTrace(
            s => s.Property("SearchPattern", searchPattern).Property("Limit", limit).Property("Skip", skip),
            "Getting file list recursively matching {Prefix} and {Pattern}...", criteria.Prefix, criteria.Pattern
        );

        await foreach (var item in _client.ListObjectsEnumAsync(
                           new ListObjectsArgs().WithBucket(_bucket).WithPrefix(criteria.Prefix).WithRecursive(true),
                           cancellationToken))
        {
            if (item.IsDir)
                continue;

            if (criteria.Pattern != null && !criteria.Pattern.IsMatch(item.Key))
            {
                _logger.LogTrace("Skipping {Path}: Doesn't match pattern", item.Key);
                continue;
            }

            list.Add(item);
        }

        if (skip.HasValue)
            list = list.Skip(skip.Value).ToList();

        if (limit.HasValue)
            list = list.Take(limit.Value).ToList();

        return list.Select(blob => new FileSpec
        {
            Path = blob.Key,
            Size = (long)blob.Size,
            Modified = DateTime.Parse(blob.LastModified),
            Created = DateTime.Parse(blob.LastModified)
        }).ToList();
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
