# coding=utf-8

"""
Collects list length from one or more keys on a single Redis server.

#### Dependencies

 * redis

Example config file RedisLlenCollector.conf:

```
enabled=True
host=my-redis-server.dc1.testo.com
port=16379
keys=myproductionqueue,events.processed,events.pending
```

#### Limitations

Auth and connecting via a UNIX socket are not supported yet.
"""


import diamond.collector

try:
    import redis
except ImportError:
    redis = None


class RedisLlenCollector(diamond.collector.Collector):

    _DATABASE_COUNT = 16
    _DEFAULT_DB = 0
    _DEFAULT_HOST = 'localhost'
    _DEFAULT_PORT = 6379
    _DEFAULT_SOCK_TIMEOUT = 5

    def get_default_config_help(self):
        """Return help text for default config

        :rtype: dict
        """
        config_help = super(RedisLlenCollector, self).get_default_config_help()
        # TODO: support auth and unix sockets in the future, like the redisstat module
        config_help.update({
            'host': 'Hostname to collect from',
            'port': 'Port number to collect from',
            'timeout': 'Socket timeout',
            'db': 'Database number to query',
            'path': 'redis',
            'keys': 'Comma-separated list of keys to collect llen from',
        })
        return config_help

    def get_default_config(self):
        """Return default config

        :rtype: dict
        """
        config = super(RedisLlenCollector, self).get_default_config()
        config.update({
            'host': self._DEFAULT_HOST,
            'port': self._DEFAULT_PORT,
            'timeout': self._DEFAULT_SOCK_TIMEOUT,
            'db': self._DEFAULT_DB,
            'path': 'redis',
            'keys': "",
        })
        return config

    def process_config(self):
        super(RedisLlenCollector, self).process_config()

    def _client(self):
        """Return a redis client for the configuration.

        :rtype: redis.Redis
        """
        try:
            client = redis.Redis(host=self.config['host'],
                                 port=int(self.config['port']),
                                 db=int(self.config['db']),
                                 socket_timeout=int(self.config['timeout']))
            client.ping()
            return client
        except Exception, ex:
            self.log.error("RedisCollector: failed to connect to %s:%i. %s.",
                           self.config['host'], self.config['port'], ex)

    def format_key_metric_path(self, key_name):
        """Returns a properly formatted metric path based on a key name.
        Full-stops in key names are replaced with double-underscore (__).

        :param str key_name: name of the key to format a metric path for
        :rtype: str
        """
        key_name = key_name.replace('.', '__')
        return "keys.{}.size".format(key_name)

    def collect(self):
        """
        Collect the stats from the redis keys and publish them.
        """
        if redis is None:
            self.log.error('Unable to import module redis')
            return {}

        client = self._client()

        for key in self.config['keys']:
            length = client.llen(key)
            self.publish_gauge(self.format_key_metric_path(key), int(length))