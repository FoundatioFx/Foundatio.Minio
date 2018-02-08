using System;
using System.Collections.Generic;
using System.Text;

namespace Foundatio.Storage {
    public class MinioFileStorageConnectionStringBuilder : MinioConnectionStringBuilder {
        private string _bucket;

        public MinioFileStorageConnectionStringBuilder() {
        }

        public MinioFileStorageConnectionStringBuilder(string connectionString) : base(connectionString) {
        }

        public string Bucket {
            get => string.IsNullOrEmpty(_bucket) ? "storage" : _bucket;
            set => _bucket = value;
        }

        protected override bool ParseItem(string key, string value) {
            if (String.Equals(key, "Bucket", StringComparison.OrdinalIgnoreCase)) {
                Bucket = value;
                return true;
            }
            return base.ParseItem(key, value);
        }

        public override string ToString() {
            var connectionString = base.ToString();
            if (!string.IsNullOrEmpty(_bucket))
                connectionString += "Bucket=" + Bucket + ";";
            return connectionString;
        }
    }
}
