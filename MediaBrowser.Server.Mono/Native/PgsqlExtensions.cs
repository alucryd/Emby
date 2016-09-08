using System;
using System.Data;
using System.Threading.Tasks;
using MediaBrowser.Model.Logging;
using Npgsql;

namespace MediaBrowser.Server.Implementations.Persistence
{
    /// <summary>
    /// Class PostgreSQLExtensions
    /// </summary>
    public class PgsqlExtensions
    {
        /// <summary>
        /// Connects to db.
        /// </summary>
        public static async Task<IDbConnection> ConnectToDb(string host, int port, string username, string password, string database, ILogger logger)
        {
            if (string.IsNullOrEmpty(host))
                {
                    throw new ArgumentNullException("host");
                }
            if (port == 0)
                {
                    throw new ArgumentNullException("port");
                }
            if (string.IsNullOrEmpty(username))
                {
                    throw new ArgumentNullException("username");
                }
            if (string.IsNullOrEmpty(password))
                {
                    throw new ArgumentNullException("password");
                }
            if (string.IsNullOrEmpty(database))
                {
                    throw new ArgumentNullException("database");
                }

            var connectionstr = new NpgsqlConnectionStringBuilder {
                Host = host,
                Port = port,
                Username = username,
                Password = password,
                Database = database
            };

            var connectionString = connectionstr.ConnectionString;

            var connection = new NpgsqlConnection(connectionString);

            await connection.OpenAsync().ConfigureAwait(false);

            return connection;
        }
    }
}

