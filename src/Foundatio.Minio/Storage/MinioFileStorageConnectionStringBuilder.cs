using System;
using System.Text;

namespace Foundatio.Storage;

public class MinioFileStorageConnectionStringBuilder : MinioConnectionStringBuilder
{
    private string? _bucket;

    public MinioFileStorageConnectionStringBuilder()
    {
    }

    public MinioFileStorageConnectionStringBuilder(string connectionString) : base(connectionString)
    {
    }

    public string Bucket
    {
        get => String.IsNullOrEmpty(_bucket) ? "storage" : _bucket;
        set => _bucket = value;
    }

    protected override bool ParseItem(string key, string value)
    {
        if (String.Equals(key, "Bucket", StringComparison.OrdinalIgnoreCase))
        {
            Bucket = value;
            return true;
        }
        return base.ParseItem(key, value);
    }

    public override string ToString()
    {
        var sb = new StringBuilder(base.ToString());
        if (!String.IsNullOrEmpty(_bucket))
            sb.Append("Bucket=").Append(Bucket).Append(';');
        return sb.ToString();
    }
}
