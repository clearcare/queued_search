from __future__ import absolute_import

import redis

from django.conf import settings

redis_connection = getattr(settings, 'QUEUE_REDIS_CONNECTION', None)
if redis_connection is None:
    raise AttributeError('QUEUE_REDIS_CONNECTION must defined in settings.')

queue_name = getattr(settings, 'SEARCH_QUEUE_NAME', 'haystack_search_queue_set')

redis_host, redis_port = redis_connection.split(':')

redis_client = redis.StrictRedis(
    host=redis_host,
    port=int(redis_port),
)


class Queue(object):

    def enqueue_delete(self, msg):
        redis_client.enqueue(queue_name, 'delete', msg)

    def enqueue_update(self, msg):
        redis_client.enqeueu(queue_name, 'update', msg)

    def enqueue(self, action, msg):
        redis_client.sadd(queue_name, '{}:{}'.format(action, msg))

    def pop(self):
        return redis_client.spop(queue_name)

    def __len__(self):
        redis_client.scard(queue_name)

queue = Queue()
