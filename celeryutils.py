"""
Wrapper for celery.task.Task that catches and logs errors.
"""
import itertools
import logging
import functools

import celery.decorators
import celery.task
from django.db import connections, transaction


log = logging.getLogger('z.celery')


class Task(celery.task.Task):

    @classmethod
    def apply_async(self, args=None, kwargs=None, **options):
        try:
            return super(Task, self).apply_async(args, kwargs, **options)
        except Exception, e:
            log.error('CELERY FAIL: %s' % e)


def task(*args, **kw):
    # Add yet another wrapper for committing transactions after the task.
    def decorate(fun):
        @functools.wraps(fun)
        def wrapped(*args, **kw):
            try:
                return fun(*args, **kw)
            finally:
                for db in connections:
                    transaction.commit_unless_managed(using=db)
        # Force usage of our Task subclass.
        kw['base'] = Task
        return celery.decorators.task(**kw)(wrapped)
    if args:
        return decorate(*args)
    else:
        return decorate


def chunked(seq, n):
    """
    Yield successive n-sized chunks from seq.

    >>> for group in chunked(range(8), 3):
    ...     print group
    [0, 1, 2]
    [3, 4, 5]
    [6, 7]

    Useful for feeding celery a list of n items at a time.
    """
    seq = iter(seq)
    while 1:
        rv = list(itertools.islice(seq, 0, n))
        if not rv:
            break
        yield rv

