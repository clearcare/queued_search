from queues import queues

from django.conf import settings
from django.db import models

from haystack import connections
from haystack.exceptions import NotHandled
from haystack.signals import BaseSignalProcessor
from haystack.utils import default_get_identifier

from queued_search.utils import get_queue_name

SKIP_NOINDEX = getattr(settings, 'SEARCH_QUEUE_SKIP_NOINDEX', True)


class QueuedSignalProcessor(BaseSignalProcessor):
    def setup(self):
        models.signals.post_save.connect(self.enqueue_save)
        models.signals.post_delete.connect(self.enqueue_delete)

    def teardown(self):
        models.signals.post_save.disconnect(self.enqueue_save)
        models.signals.post_delete.disconnect(self.enqueue_delete)

    def enqueue_save(self, sender, instance, **kwargs):
        return self.enqueue('update', instance)

    def enqueue_delete(self, sender, instance, **kwargs):
        return self.enqueue('delete', instance)

    def enqueue(self, action, instance):
        """
        Shoves a message about how to update the index into the queue.

        This is a standardized string, resembling something like::

            ``update:notes.note.23``
            # ...or...
            ``delete:weblog.entry.8``
        """
        if SKIP_NOINDEX:
            # Check if the model even has a ``SearchIndex`` implementation.
            try:
                connections['default'].get_unified_index().get_index(instance.__class__)
            except NotHandled:
                return False

        message = "%s:%s" % (action, default_get_identifier(instance))
        queue = queues.Queue(get_queue_name())
        return queue.write(message)
