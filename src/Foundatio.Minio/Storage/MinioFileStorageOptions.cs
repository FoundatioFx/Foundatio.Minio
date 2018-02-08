using System;

namespace Foundatio.Storage {
    public class MinioFileStorageOptions : SharedOptions {
        public string ConnectionString { get; set; }
    }

    public class MinioFileStorageOptionsBuilder : SharedOptionsBuilder<MinioFileStorageOptions, MinioFileStorageOptionsBuilder> {
        public MinioFileStorageOptionsBuilder ConnectionString(string connectionString) {
            if (String.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException(nameof(connectionString));
            Target.ConnectionString = connectionString;
            return this;
        }
    }
}
