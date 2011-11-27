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

        private int _database;
        private JavaScriptSerializer _serializer; 

        public RedisMessageStore(int database, string host = "localhost", int port = 6379, string password = null) 
            : this (new RedisConnection(host, port, password: password), database) { }

        private RedisMessageStore(RedisConnection redisConnection, int database)
        {
            if (_redisConnection == null)
            {
                lock (_connectionLock)
                {
                    if (_redisConnection == null)
                    {
                        _redisConnection = redisConnection;
                        _redisConnection.Open();
                    }
                }
            }
            
            _database = database;
            _serializer = new JavaScriptSerializer();
        }

        public Task<long?> GetLastId()
        {
            return _redisConnection.Strings.GetString(_database, MessageIdKey)
                .ContinueWith(t =>
                                  {
                                      long id;
                                      Int64.TryParse(t.Result, out id);
                                      return (long?)id;
                                  });
        }

        public Task Save(string key, object value)
        {
            return _redisConnection.Strings.Increment(_database, MessageIdKey)
                .ContinueWith(t =>
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
                                  });
        }

        public Task<IEnumerable<Message>> GetAllSince(string key, long id)
        {
            return _redisConnection.SortedSets.Range(_database,
                                                     key,
                                                     (double) id,
                                                     (double) long.MaxValue,
                                                     minInclusive: false)
                .ContinueWith(t =>
                              t.Result
                                  .Select(o => ProtoMessage.Deserialize(o.Key))
                                  .Select(o => new Message(o.SignalKey,
                                                           o.Id,
                                                           o.SignalKey.EndsWith("__SIGNALRCOMMAND__")
                                                               ? o.Value
                                                               : _serializer.DeserializeObject(o.Value),
                                                           o.Created)));
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