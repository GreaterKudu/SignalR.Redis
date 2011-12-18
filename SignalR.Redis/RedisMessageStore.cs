using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using BookSleeve;
using ProtoBuf;

namespace SignalR.Redis
{
    public class RedisMessageStore : IMessageStore
    {
        private const string MessageIdKey = "SignalR.MessageId";

        private static object _connectionLock = new object();
        private static RedisConnection _redisConnection;

        private static string Host { get; set; }
        private static int Port { get; set; }
        private static string Password { get; set; }

        private static RedisConnection Connection
        {
            get
            {
                return _redisConnection;
            }
        }

        private int _database;
        private JavaScriptSerializer _serializer;

        public RedisMessageStore(int database, string host = "localhost", int port = 6379, string password = null)
        {
            _database = database;
            Host = host;
            Port = port;
            Password = password;
            _serializer = new JavaScriptSerializer();
        }

        private bool ConnectionReady()
        {
            if (_redisConnection == null || _redisConnection.State != RedisConnectionBase.ConnectionState.Open)
            {
                lock (_connectionLock)
                {
                    if (_redisConnection == null || _redisConnection.State != RedisConnectionBase.ConnectionState.Open)
                    {
                        _redisConnection = new RedisConnection(Host, Port, password: Password, maxUnsent: 1);

                        try
                        {
                            _redisConnection.Open().Wait();
                            return true;                
                        } 
                        catch (Exception ex)
                        {
                            return false;
                        }
                    }
                }
            }

            return true;
        }

        public Task<long?> GetLastId()
        {
            if (ConnectionReady())
            {
                return Connection.Strings.GetString(_database, MessageIdKey)
                    .Then(t =>
                              {
                                  long id;
                                  Int64.TryParse(t.Result, out id);
                                  return (long?) id;
                              }).Catch();
            }

            throw new InvalidOperationException("Could not get latest messageId.  Redis connection failure.");
        }

        public Task Save(string key, object value)
        {
            if (ConnectionReady())
            {
                return Connection.Strings.Increment(_database, MessageIdKey)
                    .Then(t =>
                              {
                                  var message = new ProtoMessage
                                                    {
                                                        Created = DateTime.Now,
                                                        SignalKey = key,
                                                        Id = t.Result,
                                                        Value = _serializer.Serialize(value)
                                                    };
                                  _redisConnection.SortedSets.Add(_database,
                                                                  key,
                                                                  message.Serialize(),
                                                                  message.Id);
                              }).Catch();
            }

            throw new InvalidOperationException("Could not save message.  Redis connection failure.");
        }

        public Task<IEnumerable<Message>> GetAllSince(string key, long id)
        {
            if (ConnectionReady())
            {
                return Connection
                    .SortedSets.Range(_database,
                                      key,
                                      (double) id,
                                      (double) long.MaxValue,
                                      minInclusive: false)
                    .Then(t =>
                          t.Result
                              .Select(o => ProtoMessage.Deserialize(o.Key))
                              .Select(o => new Message(o.SignalKey,
                                                       o.Id,
                                                       o.SignalKey.EndsWith("__SIGNALRCOMMAND__")
                                                           ? o.Value
                                                           : _serializer.DeserializeObject(o.Value),
                                                       o.Created))).Catch();
            }

            throw new InvalidOperationException("Could not get messages.  Redis connection failure.");
        }
    }

    [ProtoContract]
    public class ProtoMessage
    {
        public static ProtoMessage Deserialize(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            {
                return Serializer.Deserialize<ProtoMessage>(ms);
            }
        }

        public byte[] Serialize()
        {
            using (var ms = new MemoryStream())
            {
                Serializer.Serialize(ms, this);
                return ms.ToArray();
            }
        }

        [ProtoMember(1)]
        public string SignalKey { get; set; }

        [ProtoMember(2)]
        public string Value { get; set; }

        [ProtoMember(3)]
        public long Id { get; set; }

        [ProtoMember(4)]
        public DateTime Created { get; set; }
    }
}