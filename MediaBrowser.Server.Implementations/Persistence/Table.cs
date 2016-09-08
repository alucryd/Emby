using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using Npgsql;

namespace MediaBrowser.Server.Implementations
{
    public class Table
    {
        public string Name { get; set; }

        public Column[] PrimaryKeys { get; set; }

        public Column[] Columns { get; set; }

        private IDbCommand _insertCommand = (IDbCommand)new NpgsqlCommand();

        public string CreateQuery {
            get {
                var columns = new List<string>();
                foreach (var column in this.PrimaryKeys)
                    {
                        columns.Add(String.Join(" ", new string[] {
                            column.Name,
                            column.ColumnType.GetPgsqlType()
                        }));
                    }
                foreach (var column in this.Columns)
                    {
                        columns.Add(String.Join(" ", new string[] {
                            column.Name,
                            column.ColumnType.GetPgsqlType(),
                            column.NotNull ? "NOT NULL" : "NULL"
                        }));
                    }
                if (this.PrimaryKeys.Length > 0)
                    {
                        columns.Add(String.Format(
                            "PRIMARY KEY ({0})", 
                            String.Join(", ", this.PrimaryKeys.Select(c => c.Name).ToArray())
                        ));
                    }

                return String.Format(
                    "CREATE TABLE IF NOT EXISTS {0} ({1})", 
                    this.Name, 
                    String.Join(", ", columns)
                );
            }
        }

        public IDbCommand InsertCommand { get { return this._insertCommand; } }

        public Table(string name, Column[] primaryKeys, Column[] columns)
        {
            this.Name = name;
            this.PrimaryKeys = primaryKeys;
            this.Columns = columns;
            
            // Build the insert command
            if (this.PrimaryKeys.Length > 0)
                {
                    var update = new List<string>();
                    foreach (var column in this.Columns)
                        {
                            update.Add(String.Join(" = ", column.Name, column.ParameterName));
                        }
                                
                    _insertCommand.CommandText = String.Format(
                        "INSERT INTO {0} ({1}, {2}) VALUES ({3}, {4}) ON CONFLICT ({1}) DO UPDATE SET {5}",
                        this.Name, 
                        String.Join(", ", this.PrimaryKeys.Select(c => c.Name).ToArray()), 
                        String.Join(", ", this.Columns.Select(c => c.Name).ToArray()), 
                        String.Join(", ", this.PrimaryKeys.Select(c => c.ParameterName).ToArray()), 
                        String.Join(", ", this.Columns.Select(c => c.ParameterName).ToArray()), 
                        String.Join(", ", update)
                    );
                } else
                {
                    _insertCommand.CommandText = String.Format(
                        "INSERT INTO {0} ({1}) VALUES ({2})",
                        this.Name, 
                        String.Join(", ", this.Columns.Select(c => c.Name).ToArray()), 
                        String.Join(", ", this.Columns.Select(c => c.ParameterName).ToArray())
                    );
                }

            foreach (var column in this.PrimaryKeys)
                {
                    var param = new NpgsqlParameter();
                    param.NpgsqlDbType = column.ColumnType.GetPgsqlDbType();
                    param.ParameterName = column.ParameterName;
                    _insertCommand.Parameters.Add(param);
                }
            foreach (var column in this.Columns)
                {
                    var param = new NpgsqlParameter();
                    param.NpgsqlDbType = column.ColumnType.GetPgsqlDbType();
                    param.ParameterName = column.ParameterName;
                    _insertCommand.Parameters.Add(param);
                }
        }
    }
}
