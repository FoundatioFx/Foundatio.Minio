using System;
using System.Threading.Tasks;
using Foundatio.Storage;
using Foundatio.Tests.Storage;
using Foundatio.Tests.Utility;
using Xunit;

namespace Foundatio.Minio.Tests.Storage;

public class MinioFileStorageTests : FileStorageTestsBase
{
    private const string BUCKET_NAME = "foundatio";

    public MinioFileStorageTests(ITestOutputHelper output) : base(output) { }

    protected override IFileStorage? GetStorage()
    {
        var section = Configuration.GetSection("Minio");
        var connectionStringBuilder = new MinioFileStorageConnectionStringBuilder
        {
            AccessKey = section["ACCESS_KEY_ID"],
            SecretKey = section["SECRET_ACCESS_KEY"],
            EndPoint = section["ENDPOINT"],
            Bucket = BUCKET_NAME
        };
        if (String.IsNullOrEmpty(connectionStringBuilder.AccessKey) || String.IsNullOrEmpty(connectionStringBuilder.SecretKey))
            return null;

        return new MinioFileStorage(o => o.ConnectionString(connectionStringBuilder.ToString()).AutoCreateBuckets().LoggerFactory(Log));
    }

    [Fact]
    public override Task CanGetEmptyFileListOnMissingDirectoryAsync()
    {
        return base.CanGetEmptyFileListOnMissingDirectoryAsync();
    }

    [Fact]
    public override Task CanGetFileListForSingleFolderAsync()
    {
        return base.CanGetFileListForSingleFolderAsync();
    }

    [Fact]
    public override Task CanGetFileListForSingleFileAsync()
    {
        return base.CanGetFileListForSingleFileAsync();
    }

    [Fact]
    public override Task CanGetPagedFileListForSingleFolderAsync()
    {
        return base.CanGetPagedFileListForSingleFolderAsync();
    }

    [Fact]
    public override Task CanGetFileInfoAsync()
    {
        return base.CanGetFileInfoAsync();
    }

    [Fact]
    public override Task CanGetNonExistentFileInfoAsync()
    {
        return base.CanGetNonExistentFileInfoAsync();
    }

    [Fact]
    public override Task CanSaveFilesAsync()
    {
        return base.CanSaveFilesAsync();
    }

    [Fact]
    public override Task CanManageFilesAsync()
    {
        return base.CanManageFilesAsync();
    }

    [Fact]
    public override Task CanRenameFilesAsync()
    {
        return base.CanRenameFilesAsync();
    }

    [Fact]
    public override Task CanConcurrentlyManageFilesAsync()
    {
        return base.CanConcurrentlyManageFilesAsync();
    }

    [Fact]
    public override void CanUseDataDirectory()
    {
        base.CanUseDataDirectory();
    }

    [Fact]
    public override Task CanDeleteEntireFolderAsync()
    {
        return base.CanDeleteEntireFolderAsync();
    }

    [Fact]
    public override Task CanDeleteEntireFolderWithWildcardAsync()
    {
        return base.CanDeleteEntireFolderWithWildcardAsync();
    }

    [Fact]
    public override Task CanDeleteFolderWithMultiFolderWildcardsAsync()
    {
        return base.CanDeleteFolderWithMultiFolderWildcardsAsync();
    }

    [Fact]
    public override Task CanDeleteSpecificFilesAsync()
    {
        return base.CanDeleteSpecificFilesAsync();
    }

    [Fact]
    public override Task CanDeleteNestedFolderAsync()
    {
        return base.CanDeleteNestedFolderAsync();
    }

    [Fact]
    public override Task CanDeleteSpecificFilesInNestedFolderAsync()
    {
        return base.CanDeleteSpecificFilesInNestedFolderAsync();
    }

    [Fact]
    public override Task CanRoundTripSeekableStreamAsync()
    {
        return base.CanRoundTripSeekableStreamAsync();
    }

    [Fact]
    public override Task WillRespectStreamOffsetAsync()
    {
        return base.WillRespectStreamOffsetAsync();
    }

    [Fact(Skip = "Write Stream is not yet supported")]
    public override Task WillWriteStreamContentAsync()
    {
        return base.WillWriteStreamContentAsync();
    }

    [Fact]
    public override Task CanSaveOverExistingStoredContent()
    {
        return base.CanSaveOverExistingStoredContent();
    }

    [Fact]
    public override Task CopyFileAsync_WithExistingFile_CreatesIdenticalCopy()
    {
        return base.CopyFileAsync_WithExistingFile_CreatesIdenticalCopy();
    }

    [Fact]
    public override Task CopyFileAsync_WithNonExistentSource_ReturnsFalse()
    {
        return base.CopyFileAsync_WithNonExistentSource_ReturnsFalse();
    }

    [Fact(Skip = "Minio/S3 DELETE is idempotent and returns success even for non-existent files")]
    public override Task DeleteFileAsync_WhenFileDoesNotExist_ReturnsFalse()
    {
        return base.DeleteFileAsync_WhenFileDoesNotExist_ReturnsFalse();
    }

    [Fact]
    public override Task DeleteFilesAsync_WithFileSpecCollection_DeletesSpecifiedFiles()
    {
        return base.DeleteFilesAsync_WithFileSpecCollection_DeletesSpecifiedFiles();
    }

    [Fact]
    public override Task GetFileContentsRawAsync_WithExistingFile_ReturnsByteArray()
    {
        return base.GetFileContentsRawAsync_WithExistingFile_ReturnsByteArray();
    }

    [Fact]
    public override Task GetFileStreamAsync_WithNonExistentFileInReadMode_ReturnsNull()
    {
        return base.GetFileStreamAsync_WithNonExistentFileInReadMode_ReturnsNull();
    }

    [Fact]
    public override Task RenameFileAsync_WhenSourceDoesNotExist_ReturnsFalse()
    {
        return base.RenameFileAsync_WhenSourceDoesNotExist_ReturnsFalse();
    }
}
