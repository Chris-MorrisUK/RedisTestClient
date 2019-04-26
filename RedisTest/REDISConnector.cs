using StackExchange.Redis;
using System;
using System.IO;
using System.Runtime.Serialization.Json;


namespace RedisTest
{


    public class REDISConnector
    {
        private ConnectionMultiplexer redis;
        private readonly object redisLock = new object();
        private readonly string host;

        public REDISConnector(string Host)
        {
            host = Host;
        }

        /// <summary>
        /// You should hold onto the result of this, rather than keep waiting for this to get a lock
        /// </summary>
        /// <returns></returns>
        public ConnectionMultiplexer GetRedis()
        {
            if (redis != null) return redis;
            lock (redisLock)
            {
                if (redis == null)
                {
                    redis = ConnectionMultiplexer.Connect(host);
                }
                return redis;
            }
        }

        public string GetValue(string key)
        {
            IDatabase redisDB = getRedisDB();
            string result = redisDB.StringGet(key);
            return result;
        }
        public byte[] GetValueBytes(string key)
        {
            IDatabase redisDB = getRedisDB();
            byte[] result = redisDB.StringGet(key);
            return result;
        }

        public bool SetValue(string key, string value)
        {
            IDatabase redisDB = getRedisDB();
            return redisDB.StringSet(key, value);
        }

        public byte[] GetValue(byte[] key)
        {
            IDatabase redisDB = getRedisDB();
            RedisValue result = redisDB.StringGet(key);
            return result;
        }

        public bool SetValue(byte[] key, byte[] value)
        {
            IDatabase redisDB = getRedisDB();
            return redisDB.StringSet(key, value);
        }

        public bool SetValue(string key, byte[] value, TimeSpan timeout)
        {
            IDatabase redisDB = getRedisDB();
            return redisDB.StringSet(key, value, timeout);
        }


        private IDatabase getRedisDB()
        {
            if (redis == null) GetRedis();
            IDatabase redisDB = redis.GetDatabase();
            return redisDB;
        }


        public bool SerializeAndSetValue(string key, object value, TimeSpan? timeout = null)
        {
            DataContractJsonSerializer serializer = new DataContractJsonSerializer(value.GetType());
            IDatabase redisDB = getRedisDB();
            bool result = false;
            using (MemoryStream ms = new MemoryStream())
            {
                serializer.WriteObject(ms, value);
                result = redisDB.StringSet(key, ms.ToArray(), timeout);
            }
            return result;
        }

        //public static bool SerializeAndSetValue(IRedisAccessable value, TimeSpan? timeout = null)
        //{
        //    return SerializeAndSetValue(value.RedisKey, value, timeout);
        //}

        public TReturn GetDeserializedValue<TReturn>(string key) where TReturn : class
        {
            DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(TReturn));

            IDatabase redisDB = getRedisDB();
            byte[] found = redisDB.StringGet(key);
            if ((found == null) || (found.LongLength == 0))
                return null;
            object result = null;
            using (MemoryStream ms = new MemoryStream(found))
                result = serializer.ReadObject(ms);

            return result as TReturn;
        }
        public bool IsItThere()
        {
            return getRedisDB() != null;
        }

        public bool CheckForExistance(string key, bool retry = false)
        {
            try
            {
                IDatabase redisDB = getRedisDB();
                return redisDB.KeyExists(key);
            }
            catch (TimeoutException)
            {
                System.Threading.Thread.Sleep(100);//let the queue subside
                if (!retry)//try again
                    return CheckForExistance(key, true);
                else
                    return false;

            }
        }
    }


}
