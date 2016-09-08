using MediaBrowser.Controller;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Persistence;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace MediaBrowser.Server.Implementations.Persistence
{
    /// <summary>
    /// Class PostgreSQLUserRepository
    /// </summary>
    public class PgsqlUserRepository : BasePgsqlRepository, IUserRepository
    {
        private readonly IJsonSerializer _jsonSerializer;

        private readonly Table _users;
        private readonly Column _guid;
        private readonly Column _data;
        private readonly Index _idx;

        public PgsqlUserRepository(ILogManager logManager, IServerApplicationPaths appPaths, IJsonSerializer jsonSerializer, IDbConnector dbConnector)
            : base(logManager, dbConnector)
        {
            _jsonSerializer = jsonSerializer;

            host = "localhost";
            port = 5432;
            username = "emby";
            password = "emby";
            database = "emby";

            _guid = new Column("Guid", ColumnType.UniqueIdentifier);
            _data = new Column("Data", ColumnType.Binary);

            _users = new Table("Users",
                new Column[] {
                    _guid
                },
                new Column[] {
                    _data
                });

            _idx = new Index("idx_users_guid", _users, new Column[] { _guid });
                
        }

        /// <summary>
        /// Gets the name of the repository
        /// </summary>
        /// <value>The name.</value>
        public string Name {
            get {
                return "PostgreSQL";
            }
        }

        /// <summary>
        /// Opens the connection to the database
        /// </summary>
        /// <returns>Task.</returns>
        public async Task Initialize()
        {
            using (var connection = await CreateConnection().ConfigureAwait(false))
                {
                    string[] queries = {
                        _users.CreateQuery,
                        _idx.CreateQuery,
                    };
                    connection.RunQueries(queries, Logger);
                }
        }

        /// <summary>
        /// Save a user in the repo
        /// </summary>
        /// <param name="user">The user.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>Task.</returns>
        /// <exception cref="System.ArgumentNullException">user</exception>
        public async Task SaveUser(User user, CancellationToken cancellationToken)
        {
            if (user == null)
                {
                    throw new ArgumentNullException("user");
                }

            cancellationToken.ThrowIfCancellationRequested();

            var serialized = _jsonSerializer.SerializeToBytes(user);

            cancellationToken.ThrowIfCancellationRequested();

            using (var connection = await CreateConnection().ConfigureAwait(false))
                {
                    IDbTransaction transaction = null;

                    try
                        {
                            transaction = connection.BeginTransaction();

                            using (var cmd = _users.InsertCommand)
                                {
                                    cmd.Connection = connection;

                                    var index = 0;

                                    cmd.GetParameter(index++).Value = user.Id;
                                    cmd.GetParameter(index++).Value = serialized;

                                    cmd.Transaction = transaction;

                                    cmd.ExecuteNonQuery();
                                }

                            transaction.Commit();
                        } catch (OperationCanceledException)
                        {
                            if (transaction != null)
                                {
                                    transaction.Rollback();
                                }

                            throw;
                        } catch (Exception e)
                        {
                            Logger.ErrorException("Failed to save user:", e);

                            if (transaction != null)
                                {
                                    transaction.Rollback();
                                }

                            throw;
                        } finally
                        {
                            if (transaction != null)
                                {
                                    transaction.Dispose();
                                }
                        }
                }
        }

        /// <summary>
        /// Retrieve all users from the database
        /// </summary>
        /// <returns>IEnumerable{User}.</returns>
        public IEnumerable<User> RetrieveAllUsers()
        {
            var list = new List<User>();

            using (var connection = CreateConnection().Result)
                {
                    using (var cmd = (IDbCommand)new NpgsqlCommand())
                        {
                            cmd.Connection = connection;
                            cmd.CommandText = "SELECT guid, data FROM users";

                            using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult))
                                {
                                    while (reader.Read())
                                        {
                                            var id = reader.GetGuid(0);

                                            using (var stream = reader.GetMemoryStream(1))
                                                {
                                                    var user = _jsonSerializer.DeserializeFromStream<User>(stream);
                                                    user.Id = id;
                                                    list.Add(user);
                                                }
                                        }
                                }
                        }
                }

            return list;
        }

        /// <summary>
        /// Deletes the user.
        /// </summary>
        /// <param name="user">The user.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>Task.</returns>
        /// <exception cref="System.ArgumentNullException">user</exception>
        public async Task DeleteUser(User user, CancellationToken cancellationToken)
        {
            if (user == null)
                {
                    throw new ArgumentNullException("user");
                }

            cancellationToken.ThrowIfCancellationRequested();

            using (var connection = await CreateConnection().ConfigureAwait(false))
                {
                    IDbTransaction transaction = null;

                    try
                        {
                            transaction = connection.BeginTransaction();

                            using (var cmd = (IDbCommand)new NpgsqlCommand())
                                {
                                    cmd.Connection = connection;
                                    cmd.CommandText = "DELETE FROM users WHERE guid = @guid";

                                    cmd.Parameters.Add(cmd, "@guid", DbType.Guid).Value = user.Id;

                                    cmd.Transaction = transaction;

                                    cmd.ExecuteNonQuery();
                                }

                            transaction.Commit();
                        } catch (OperationCanceledException)
                        {
                            if (transaction != null)
                                {
                                    transaction.Rollback();
                                }

                            throw;
                        } catch (Exception e)
                        {
                            Logger.ErrorException("Failed to delete user:", e);

                            if (transaction != null)
                                {
                                    transaction.Rollback();
                                }

                            throw;
                        } finally
                        {
                            if (transaction != null)
                                {
                                    transaction.Dispose();
                                }
                        }
                }
        }
    }
}