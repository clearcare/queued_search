from __future__ import absolute_import

import redis

from django.conf import settings

redis_connection = getattr(settings, 'QUEUE_REDIS_CONNECTION', 'localhost:6379')
queue_name = getattr(settings, 'SEARCH_QUEUE_NAME', 'haystack_search_queue_set')

redis_host, redis_port = redis_connection.split(':')

redis_client = redis.StrictRedis(
    host=redis_host,
    port=int(redis_port),
)


class Queue(object):

    def enqueue(self, action, msg):
        redis_client.sadd(queue_name, '{}:{}'.format(action, msg))

    def enqueue_delete(self, msg):
        self.enqueue('delete', msg)

    def enqueue_update(self, msg):
        self.enqueue('update', msg)

    def pop(self):
        return redis_client.spop(queue_name)

    def peek_all(self):
        return redis_client.smembers(queue_name)

    def __len__(self):
        return int(redis_client.scard(queue_name))

queue = Queue()
