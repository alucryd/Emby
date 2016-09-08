using System.Data;
using System.Threading.Tasks;

namespace MediaBrowser.Server.Implementations.Persistence
{
    public interface IDbConnector
    {
        Task<IDbConnection> Connect(string dbPath, bool isReadOnly, bool enablePooling = false, int? cacheSize = null);

        Task<IDbConnection> Connect(string host, int port, string username, string password, string database);
    }
}
