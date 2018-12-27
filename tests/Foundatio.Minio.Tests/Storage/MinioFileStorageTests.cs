using System;
using System.Threading.Tasks;
using Foundatio.Storage;
using Foundatio.Tests.Storage;
using Foundatio.Tests.Utility;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Minio.Tests.Storage {
    public class MinioFileStorageTests : FileStorageTestsBase {
        public MinioFileStorageTests(ITestOutputHelper output) : base(output) { }

        protected override IFileStorage GetStorage() {
            var section = Configuration.GetSection("Minio");
            var connectionStringBuilder = new MinioFileStorageConnectionStringBuilder {
                AccessKey = section["ACCESS_KEY_ID"],
                SecretKey = section["SECRET_ACCESS_KEY"],
                EndPoint = section["ENDPOINT"],
                Bucket = "foundatio"
            };
            if (String.IsNullOrEmpty(connectionStringBuilder.AccessKey) || String.IsNullOrEmpty(connectionStringBuilder.SecretKey))
                return null;

            return new MinioFileStorage(o => o.ConnectionString(connectionStringBuilder.ToString()).LoggerFactory(Log));
        }

        [Fact]
        public override Task CanGetEmptyFileListOnMissingDirectoryAsync() {
            return base.CanGetEmptyFileListOnMissingDirectoryAsync();
        }

        [Fact]
        public override Task CanGetFileListForSingleFolderAsync() {
            return base.CanGetFileListForSingleFolderAsync();
        }

        [Fact]
        public override Task CanGetFileInfoAsync() {
            return base.CanGetFileInfoAsync();
        }

        [Fact]
        public override Task CanGetNonExistentFileInfoAsync() {
            return base.CanGetNonExistentFileInfoAsync();
        }

        [Fact]
        public override Task CanSaveFilesAsync() {
            return base.CanSaveFilesAsync();
        }

        [Fact]
        public override Task CanManageFilesAsync() {
            return base.CanManageFilesAsync();
        }

        [Fact]
        public override Task CanRenameFilesAsync() {
            return base.CanRenameFilesAsync();
        }

        [Fact]
        public override Task CanConcurrentlyManageFilesAsync() {
            return base.CanConcurrentlyManageFilesAsync();
        }

        [Fact]
        public override void CanUseDataDirectory() {
            base.CanUseDataDirectory();
        }

        [Fact]
        public override Task CanDeleteEntireFolderAsync() {
            return base.CanDeleteEntireFolderAsync();
        }

        [Fact]
        public override Task CanDeleteEntireFolderWithWildcardAsync() {
            return base.CanDeleteEntireFolderWithWildcardAsync();
        }

        [Fact]
        public override Task CanDeleteSpecificFilesAsync() {
            return base.CanDeleteSpecificFilesAsync();
        }

        [Fact]
        public override Task CanDeleteNestedFolderAsync() {
            return base.CanDeleteNestedFolderAsync();
        }

        [Fact]
        public override Task CanDeleteSpecificFilesInNestedFolderAsync() {
            return base.CanDeleteSpecificFilesInNestedFolderAsync();
        }

        [Fact]
        public override Task CanRoundTripSeekableStreamAsync() {
            return base.CanRoundTripSeekableStreamAsync();
        }

        [Fact]
        public override Task WillRespectStreamOffsetAsync() {
            return base.WillRespectStreamOffsetAsync();
        }
    }
}
