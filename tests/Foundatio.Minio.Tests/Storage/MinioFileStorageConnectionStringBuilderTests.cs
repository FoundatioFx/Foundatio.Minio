using Foundatio.Storage;
using Xunit;

namespace Foundatio.Minio.Tests.Storage
{
    public class MinioFileStorageConnectionStringBuilderTests : ConnectionStringBuilderTests
    {
        protected override MinioConnectionStringBuilder CreateConnectionStringBuilder(string connectionString)
        {
            return new MinioFileStorageConnectionStringBuilder(connectionString);
        }

        protected override MinioConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new MinioFileStorageConnectionStringBuilder();
        }

        [Fact]
        public override void InvalidKeyShouldThrow()
        {
            base.InvalidKeyShouldThrow();
        }

        [Fact]
        public override void CanParseAccessKey()
        {
            base.CanParseAccessKey();
        }

        [Fact]
        public override void CanParseSecretKey()
        {
            base.CanParseSecretKey();
        }

        [Fact]
        public override void CanParseRegion()
        {
            base.CanParseRegion();
        }

        [Fact]
        public override void CanParseEndPoint()
        {
            base.CanParseEndPoint();
        }

        [Fact]
        public override void CanGenerateConnectionString()
        {
            base.CanGenerateConnectionString();
        }

        [Fact]
        public void CanParseBucket()
        {
            foreach (var key in new[] { "Bucket", "bucket" })
            {
                var connectionStringBuilder = CreateConnectionStringBuilder($"AccessKey=TestAccessKey;SecretKey=TestSecretKey;{key}=TestBucket");
                Assert.Equal("TestAccessKey", connectionStringBuilder.AccessKey);
                Assert.Equal("TestSecretKey", connectionStringBuilder.SecretKey);
                Assert.Equal("TestBucket", ((MinioFileStorageConnectionStringBuilder)connectionStringBuilder).Bucket);
                Assert.Null(connectionStringBuilder.Region);
            }
        }

        [Fact]
        public void CanGenerateConnectionStringWithBucket()
        {
            var connectionStringBuilder = (MinioFileStorageConnectionStringBuilder)CreateConnectionStringBuilder();
            connectionStringBuilder.AccessKey = "TestAccessKey";
            connectionStringBuilder.SecretKey = "TestSecretKey";
            connectionStringBuilder.Region = "TestRegion";
            connectionStringBuilder.Bucket = "TestBucket";

            Assert.Equal("AccessKey=TestAccessKey;SecretKey=TestSecretKey;Region=TestRegion;Bucket=TestBucket;", connectionStringBuilder.ToString());
        }
    }
}
