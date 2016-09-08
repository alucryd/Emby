using System;
using System.Reflection;
using System.Data;
using NpgsqlTypes;

namespace MediaBrowser.Server.Implementations
{

    class ColumnTypeAttr: Attribute
    {
        public string SqliteType { get; private set; }

        public DbType SqliteDbType { get; private set; }

        public string PgsqlType { get; private set; }

        public NpgsqlDbType PgsqlDbType { get; private set; }

        internal ColumnTypeAttr(string pgsqlType, NpgsqlDbType pgsqlDbType)
        {
            this.PgsqlType = pgsqlType;
            this.PgsqlDbType = pgsqlDbType;
        }
    }

    public static class ColumnTypeReflection
    {
        public static DbType GetDbType(this ColumnType t)
        {
            return GetAttr(t).SqliteDbType;
        }

        public static string GetPgsqlType(this ColumnType t)
        {
            return GetAttr(t).PgsqlType;
        }

        public static NpgsqlDbType GetPgsqlDbType(this ColumnType t)
        {
            return GetAttr(t).PgsqlDbType;
        }

        private static ColumnTypeAttr GetAttr(ColumnType t)
        {
            return (ColumnTypeAttr)Attribute.GetCustomAttribute(ForValue(t), typeof(ColumnTypeAttr));
        }

        private static MemberInfo ForValue(ColumnType t)
        {
            return typeof(ColumnType).GetField(Enum.GetName(typeof(ColumnType), t));
        }

    }

    public enum ColumnType
    {
        [ColumnTypeAttr("BYTEA", NpgsqlDbType.Bytea)] Binary,
        [ColumnTypeAttr("BOOLEAN", NpgsqlDbType.Boolean)] Boolean,
        [ColumnTypeAttr("DATE", NpgsqlDbType.Date)] Date,
        [ColumnTypeAttr("REAL", NpgsqlDbType.Real)] Real,
        [ColumnTypeAttr("INTEGER", NpgsqlDbType.Integer)] Integer,
        [ColumnTypeAttr("BIGINT", NpgsqlDbType.Bigint)] BigInteger,
        [ColumnTypeAttr("TEXT", NpgsqlDbType.Text)] Text,
        [ColumnTypeAttr("TIMESTAMP", NpgsqlDbType.Timestamp)] Timestamp,
        [ColumnTypeAttr("UUID", NpgsqlDbType.Uuid)] UniqueIdentifier
    }
}
