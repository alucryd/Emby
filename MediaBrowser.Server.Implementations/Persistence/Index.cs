using System;
using System.Collections.Generic;

namespace MediaBrowser.Server.Implementations
{
    public class Index
    {
        private List<string> columns = new List<string>();

        public string Name { get; set; }

        public Table Table { get; set; }

        public Column[] Columns {
            set { 
                foreach (Column c in value)
                    {
                        columns.Add(c.Name);
                    }
            }
        }

        public string CreateQuery {
            get {
                return String.Format(
                    "CREATE INDEX IF NOT EXISTS {0} ON {1} ({2})", 
                    this.Name, 
                    this.Table.Name, 
                    String.Join(", ", this.columns)
                );
            }
        }

        public Index(string name, Table table, Column[] columns)
        {
            this.Name = name;
            this.Table = table;
            this.Columns = columns;
        }
    }
}
