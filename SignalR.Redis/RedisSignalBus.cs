using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BookSleeve;

namespace SignalR.Redis
{
    public class RedisSignalBus : ISignalBus
    {
        private static object _connectionLock = new object();
        private static RedisConnection _redisConnection;
        private static RedisSubscriberConnection _subscriberConnection;

        //Handles local signaling.
        private static InProcessSignalBus _inProcessSignalBus;

        //Keep a list of subscriptions around so if there's an error we can resubscribe.
        private static ConcurrentDictionary<string, bool> Subscriptions;

        private static string Host { get; set; }
        private static int Port { get; set; }
        private static string Password { get; set; }

        public RedisSignalBus(string host, int port, string password)
        {
            Host = host;
            Port = port;
            Password = password;
            Subscriptions = new ConcurrentDictionary<string, bool>();
            _inProcessSignalBus = new InProcessSignalBus();
            ConnectionReady();
        }

        public bool ConnectionReady()
        {
            if (_redisConnection == null || _redisConnection.State != RedisConnectionBase.ConnectionState.Open)
            {
                lock (_connectionLock)
                {
                    if (_redisConnection == null || _redisConnection.State != RedisConnectionBase.ConnectionState.Open)
                    {
                        _redisConnection = new RedisConnection(Host, Port, password: Password);
                        try
                        {
                            _redisConnection.Open();
                            _subscriberConnection = _redisConnection.GetOpenSubscriberChannel();
                            _subscriberConnection.MessageReceived += OnRedisMessageReceived;
                            _subscriberConnection.Error += OnRedisError;
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

        private void OnRedisMessageReceived(string eventKey, byte[] messageBytes)
        {
            OnSignaled(eventKey);
        }

        private void OnRedisError(object sender, ErrorEventArgs args)
        {
            var success = false;

            while (!success)
            {
                if (ConnectionReady())
                {
                    //recover subscriptions...
                    Subscriptions.Keys.ToList().ForEach(o => _subscriberConnection.Subscribe(o));
                    success = true;
                }
            }
        }

        private void OnSignaled(string eventKey)
        {
            _inProcessSignalBus.Signal(eventKey);
        }

        public Task Signal(string eventKey)
        {
            //Run our local event handlers for the key and publish the event to anyone listening.
            return
                Task.Factory.StartNew(() =>
                                          {
                                              OnSignaled(eventKey);
                                              if (ConnectionReady())
                                              {
                                                  return _redisConnection.Publish(eventKey, "", true);
                                              }
                                              else
                                              {
                                                  throw new InvalidOperationException("Could not signal: Redis connection failure.");
                                              }
                                          }).FastUnwrap();
        }

        public void AddHandler(string eventKey, EventHandler<SignaledEventArgs> handler)
        {
            _inProcessSignalBus.AddHandler(eventKey, handler);
            if (ConnectionReady() && !Subscriptions.ContainsKey(eventKey))
            {
                Subscriptions.GetOrAdd(eventKey, true);
                _subscriberConnection.Subscribe(eventKey);
            }
        }

        public void RemoveHandler(string eventKey, EventHandler<SignaledEventArgs> handler)
        {
            _inProcessSignalBus.RemoveHandler(eventKey, handler);
        }
    }
}