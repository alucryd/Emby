using MediaBrowser.Common.Configuration;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Persistence;
using MediaBrowser.Model.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MediaBrowser.Server.Implementations.Persistence
{
    public class PgsqlUserDataRepository : BasePgsqlRepository, IUserDataRepository
    {
        private IDbConnection _connection;

        private readonly Table _userData;
        private readonly Column _key;
        private readonly Column _userId;
        private readonly Column _rating;
        private readonly Column _played;
        private readonly Column _playCount;
        private readonly Column _isFavorite;
        private readonly Column _playbackPositionTicks;
        private readonly Column _lastPlayedDate;
        private readonly Column _audioStreamIndex;
        private readonly Column _subtitleStreamIndex;
        private Index _idx1;
        private Index _idx2;
        private Index _idx3;
        private Index _idx4;

        public PgsqlUserDataRepository(ILogManager logManager, IApplicationPaths appPaths, IDbConnector connector)
            : base(logManager, connector)
        {
            host = "localhost";
            port = 5432;
            username = "emby";
            password = "emby";
            database = "emby";

            _key = new Column("Key", ColumnType.Text);
            _userId = new Column("UserId", ColumnType.UniqueIdentifier);
            _rating = new Column("Rating", ColumnType.Real);
            _played = new Column("Played", ColumnType.Boolean);
            _playCount = new Column("PlayCount", ColumnType.Integer);
            _isFavorite = new Column("IsFavorite", ColumnType.Boolean);
            _playbackPositionTicks = new Column("PlaybackPositionTicks", ColumnType.BigInteger);
            _lastPlayedDate = new Column("LastPlayedDate", ColumnType.Date);
            _audioStreamIndex = new Column("AudioStreamIndex", ColumnType.Integer);
            _subtitleStreamIndex = new Column("SubtitleStreamIndex", ColumnType.Integer);

            _userData = new Table("UserData",
                new Column[] {
                    _key,
                    _userId
                },
                new Column[] {
                    _rating,
                    _played,
                    _playCount,
                    _isFavorite,
                    _playbackPositionTicks,
                    _lastPlayedDate,
                    _audioStreamIndex,
                    _subtitleStreamIndex
                });
            
            _idx1 = new Index("userdata_key_userid", _userData, new Column[] {
                _key,
                _userId
            });
            _idx2 = new Index("userdata_key_userid_played", _userData, new Column[] {
                _key,
                _userId,
                _played
            });
            _idx3 = new Index("userdata_key_userid_playbackpositionticks", _userData, new Column[] {
                _key,
                _userId,
                _playbackPositionTicks
            });
            _idx4 = new Index("userdata_key_userid_isfavorite", _userData, new Column[] {
                _key,
                _userId,
                _isFavorite
            });
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
        public async Task Initialize(IDbConnection connection, SemaphoreSlim writeLock)
        {
            WriteLock.Dispose();
            WriteLock = writeLock;
            _connection = connection;

            string[] queries = {
                _userData.CreateQuery,
                _idx1.CreateQuery,
                _idx2.CreateQuery,
                _idx3.CreateQuery,
                _idx4.CreateQuery
            };

            _connection.RunQueries(queries, Logger);
        }

        /// <summary>
        /// Saves the user data.
        /// </summary>
        /// <param name="userId">The user id.</param>
        /// <param name="key">The key.</param>
        /// <param name="userData">The user data.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>Task.</returns>
        /// <exception cref="System.ArgumentNullException">userData
        /// or
        /// cancellationToken
        /// or
        /// userId
        /// or
        /// userDataId</exception>
        public Task SaveUserData(Guid userId, string key, UserItemData userData, CancellationToken cancellationToken)
        {
            if (userData == null)
                {
                    throw new ArgumentNullException("userData");
                }
            if (userId == Guid.Empty)
                {
                    throw new ArgumentNullException("userId");
                }
            if (string.IsNullOrEmpty(key))
                {
                    throw new ArgumentNullException("key");
                }

            return PersistUserData(userId, key, userData, cancellationToken);
        }

        public Task SaveAllUserData(Guid userId, IEnumerable<UserItemData> userData, CancellationToken cancellationToken)
        {
            if (userData == null)
                {
                    throw new ArgumentNullException("userData");
                }
            if (userId == Guid.Empty)
                {
                    throw new ArgumentNullException("userId");
                }

            return PersistAllUserData(userId, userData, cancellationToken);
        }

        /// <summary>
        /// Persists the user data.
        /// </summary>
        /// <param name="userId">The user id.</param>
        /// <param name="key">The key.</param>
        /// <param name="userData">The user data.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>Task.</returns>
        public async Task PersistUserData(Guid userId, string key, UserItemData userData, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await WriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);

            IDbTransaction transaction = null;

            try
                {
                    transaction = _connection.BeginTransaction();

                    using (var cmd = _userData.InsertCommand)
                        {
                            cmd.Connection = _connection;

                            var index = 0;

                            cmd.GetParameter(index++).Value = key;
                            cmd.GetParameter(index++).Value = userId;
                            cmd.GetParameter(index++).Value = userData.Rating;
                            cmd.GetParameter(index++).Value = userData.Played;
                            cmd.GetParameter(index++).Value = userData.PlayCount;
                            cmd.GetParameter(index++).Value = userData.IsFavorite;
                            cmd.GetParameter(index++).Value = userData.PlaybackPositionTicks;
                            cmd.GetParameter(index++).Value = userData.LastPlayedDate;
                            cmd.GetParameter(index++).Value = userData.AudioStreamIndex;
                            cmd.GetParameter(index++).Value = userData.SubtitleStreamIndex;

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
                    Logger.ErrorException("Failed to save user data:", e);

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

                    WriteLock.Release();
                }
        }

        /// <summary>
        /// Persist all user data for the specified user
        /// </summary>
        /// <param name="userId"></param>
        /// <param name="userData"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task PersistAllUserData(Guid userId, IEnumerable<UserItemData> userData, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await WriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);

            IDbTransaction transaction = null;

            try
                {
                    transaction = _connection.BeginTransaction();

                    foreach (var userItemData in userData)
                        {
                            using (var cmd = _connection.CreateCommand())
                                {
                                    cmd.CommandText = "INSERT INTO userdata (key, userId, rating, played, playCount, isFavorite, playbackPositionTicks, lastPlayedDate, AudioStreamIndex, SubtitleStreamIndex) VALUES (@key, @userId, @rating, @played, @playCount, @isFavorite, @playbackPositionTicks, @lastPlayedDate, @AudioStreamIndex, @SubtitleStreamIndex) ON CONFLICT (key, userId) DO UPDATE SET rating = @rating, played = @played, playCount = @playCount, isFavorite = @isFavorite, playbackPositionTicks = @playbackPositionTicks, lastPlayedDate = @lastPlayedDate, AudioStreamIndex = @AudioStreamIndex, SubtitleStreamIndex = @SubtitleStreamIndex";

                                    cmd.Parameters.Add(cmd, "@key", DbType.String).Value = userItemData.Key;
                                    cmd.Parameters.Add(cmd, "@userId", DbType.Guid).Value = userId;
                                    cmd.Parameters.Add(cmd, "@rating", DbType.Double).Value = userItemData.Rating;
                                    cmd.Parameters.Add(cmd, "@played", DbType.Boolean).Value = userItemData.Played;
                                    cmd.Parameters.Add(cmd, "@playCount", DbType.Int32).Value = userItemData.PlayCount;
                                    cmd.Parameters.Add(cmd, "@isFavorite", DbType.Boolean).Value = userItemData.IsFavorite;
                                    cmd.Parameters.Add(cmd, "@playbackPositionTicks", DbType.Int64).Value = userItemData.PlaybackPositionTicks;
                                    cmd.Parameters.Add(cmd, "@lastPlayedDate", DbType.DateTime).Value = userItemData.LastPlayedDate;
                                    cmd.Parameters.Add(cmd, "@AudioStreamIndex", DbType.Int32).Value = userItemData.AudioStreamIndex;
                                    cmd.Parameters.Add(cmd, "@SubtitleStreamIndex", DbType.Int32).Value = userItemData.SubtitleStreamIndex;

                                    cmd.Transaction = transaction;

                                    cmd.ExecuteNonQuery();
                                }

                            cancellationToken.ThrowIfCancellationRequested();
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
                    Logger.ErrorException("Failed to save user data:", e);

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

                    WriteLock.Release();
                }
        }

        /// <summary>
        /// Gets the user data.
        /// </summary>
        /// <param name="userId">The user id.</param>
        /// <param name="key">The key.</param>
        /// <returns>Task{UserItemData}.</returns>
        /// <exception cref="System.ArgumentNullException">
        /// userId
        /// or
        /// key
        /// </exception>
        public UserItemData GetUserData(Guid userId, string key)
        {
            if (userId == Guid.Empty)
                {
                    throw new ArgumentNullException("userId");
                }
            if (string.IsNullOrEmpty(key))
                {
                    throw new ArgumentNullException("key");
                }

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT key, userid, rating, played, playCount, isFavorite, playbackPositionTicks, lastPlayedDate, AudioStreamIndex, SubtitleStreamIndex FROM userdata WHERE key = @key AND userId = @userId";

                    cmd.Parameters.Add(cmd, "@key", DbType.String).Value = key;
                    cmd.Parameters.Add(cmd, "@userId", DbType.Guid).Value = userId;

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow))
                        {
                            if (reader.Read())
                                {
                                    return ReadRow(reader);
                                }
                        }

                    return null;
                }
        }

        public UserItemData GetUserData(Guid userId, List<string> keys)
        {
            if (userId == Guid.Empty)
                {
                    throw new ArgumentNullException("userId");
                }
            if (keys == null)
                {
                    throw new ArgumentNullException("keys");
                }

            using (var cmd = _connection.CreateCommand())
                {
                    var index = 0;
                    var userdataKeys = new List<string>();
                    var builder = new StringBuilder();
                    foreach (var key in keys)
                        {
                            var paramName = "@key" + index;
                            userdataKeys.Add("key = " + paramName);
                            cmd.Parameters.Add(cmd, paramName, DbType.String).Value = key;
                            builder.Append(" WHEN key = " + paramName + " THEN " + index);
                            index++;
                            break;
                        }

                    var keyText = string.Join(" OR ", userdataKeys.ToArray());

                    cmd.CommandText = "SELECT key, userid, rating, played, playCount, isFavorite, playbackPositionTicks, lastPlayedDate, AudioStreamIndex, SubtitleStreamIndex FROM userdata WHERE userId = @userId AND (" + keyText + ") ";

                    cmd.CommandText += " ORDER BY (CASE " + builder + " ELSE " + keys.Count.ToString(CultureInfo.InvariantCulture) + " END)";
                    cmd.CommandText += " LIMIT 1";

                    cmd.Parameters.Add(cmd, "@userId", DbType.Guid).Value = userId;

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow))
                        {
                            if (reader.Read())
                                {
                                    return ReadRow(reader);
                                }
                        }

                    return null;
                }
        }

        /// <summary>
        /// Return all user-data associated with the given user
        /// </summary>
        /// <param name="userId"></param>
        /// <returns></returns>
        public IEnumerable<UserItemData> GetAllUserData(Guid userId)
        {
            if (userId == Guid.Empty)
                {
                    throw new ArgumentNullException("userId");
                }

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT key, userid, rating, played, playCount, isFavorite, playbackPositionTicks, lastPlayedDate, AudioStreamIndex, SubtitleStreamIndex FROM userdata WHERE userId = @userId";

                    cmd.Parameters.Add(cmd, "@userId", DbType.Guid).Value = userId;

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult))
                        {
                            while (reader.Read())
                                {
                                    yield return ReadRow(reader);
                                }
                        }
                }
        }

        /// <summary>
        /// Read a row from the specified reader into the provided userData object
        /// </summary>
        /// <param name="reader"></param>
        private UserItemData ReadRow(IDataReader reader)
        {
            var userData = new UserItemData();

            userData.Key = reader.GetString(0);
            userData.UserId = reader.GetGuid(1);

            if (!reader.IsDBNull(2))
                {
                    userData.Rating = reader.GetDouble(2);
                }

            userData.Played = reader.GetBoolean(3);
            userData.PlayCount = reader.GetInt32(4);
            userData.IsFavorite = reader.GetBoolean(5);
            userData.PlaybackPositionTicks = reader.GetInt64(6);

            if (!reader.IsDBNull(7))
                {
                    userData.LastPlayedDate = reader.GetDateTime(7).ToUniversalTime();
                }

            if (!reader.IsDBNull(8))
                {
                    userData.AudioStreamIndex = reader.GetInt32(8);
                }

            if (!reader.IsDBNull(9))
                {
                    userData.SubtitleStreamIndex = reader.GetInt32(9);
                }

            return userData;
        }

        protected override void Dispose(bool dispose)
        {
            // handled by library database
        }

        protected override void CloseConnection()
        {
            // handled by library database
        }
    }
}