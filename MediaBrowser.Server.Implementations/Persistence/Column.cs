using System;
using System.Data;

namespace MediaBrowser.Server.Implementations
{
    public class Column
    {
        private string _name;

        public string Name { get { return this._name; } set { this._name = value; } }

        public string ParameterName { get { return "@" + this._name; } }

        public ColumnType ColumnType { get; set; }

        public bool NotNull { get; set; } = false;

        public Column(string name, ColumnType columnType)
        {
            this.Name = name;
            this.ColumnType = columnType;
        }

        public Column(string name, ColumnType columnType, bool notNull)
        {
            this.Name = name;
            this.ColumnType = columnType;
            this.NotNull = notNull;
        }
    }
}
