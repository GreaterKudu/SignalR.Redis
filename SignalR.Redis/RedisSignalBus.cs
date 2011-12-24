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
        private static ConcurrentDictionary<string, SafeSet<EventHandler<SignaledEventArgs>>> _handlers;

        private static string Host { get; set; }
        private static int Port { get; set; }
        private static string Password { get; set; }

        public RedisSignalBus(string host = "localhost", int port = 6379, string password = null)
        {
            Host = host;
            Port = port;
            Password = password;
            _handlers = new ConcurrentDictionary<string, SafeSet<EventHandler<SignaledEventArgs>>>();
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
                    _handlers.Keys.ToList().ForEach(o => _subscriberConnection.Subscribe(o));
                    success = true;
                }
            }
        }

        private void OnSignaled(string eventKey)
        {
            //Get handlers for this event...
            SafeSet<EventHandler<SignaledEventArgs>> handlersForEvent;
            if (_handlers.TryGetValue(eventKey, out handlersForEvent))
            {
                Parallel.ForEach(handlersForEvent.GetSnapshot(), h => h(this, new SignaledEventArgs(eventKey)));
            }
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
            //Get or create the bag of handlers for the event named {eventKey}
            var eventHandlersForKey = _handlers.GetOrAdd(eventKey, new SafeSet<EventHandler<SignaledEventArgs>>());

            //Determine whether we need to create a new redis subscription.
            var newRedisSubscriptionRequired = !eventHandlersForKey.GetSnapshot().Any();

            //Add the handler to the bag and create the redis subscription if necessary
            eventHandlersForKey.Add(handler);
            if (newRedisSubscriptionRequired)
            {
                if (ConnectionReady())
                {
                    _subscriberConnection.Subscribe(eventKey);    
                }
            }
        }

        public void RemoveHandler(string eventKey, EventHandler<SignaledEventArgs> handler)
        {
            SafeSet<EventHandler<SignaledEventArgs>> eventHandlersForKey;
            if (_handlers.TryGetValue(eventKey, out eventHandlersForKey))
            {
                //We had a subscription, remove the handler.
                eventHandlersForKey.Remove(handler);

                //If there are no more handlers for this event unsubscribe our redis connection.
                if (!eventHandlersForKey.GetSnapshot().Any())
                {
                    _handlers.TryRemove(eventKey, out eventHandlersForKey);
                    if (ConnectionReady())
                    {
                        _subscriberConnection.Unsubscribe(eventKey);    
                    }
                }
            }
        }
    }
}