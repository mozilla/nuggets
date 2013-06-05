"""
Wrapper for celery.task.Task that catches and logs errors.
"""
import itertools
import logging
import functools
import sys

import celery.task
from celery.signals import task_failure
from django.db import connections, transaction


log = logging.getLogger('z.celery')


class Task(celery.task.Task):

    @classmethod
    def apply_async(self, args=None, kwargs=None, **options):
        try:
            return super(Task, self).apply_async(args, kwargs, **options)
        except Exception, e:
            log.error('CELERY FAIL: %s' % e)
            raise


def task(*args, **kw):
    # Add yet another wrapper for committing transactions after the task.
    def decorate(fun):
        @functools.wraps(fun)
        def wrapped(*args, **kw):
            was_exception = False
            try:
                return fun(*args, **kw)
            finally:
                try:
                    for db in connections:
                        transaction.commit_unless_managed(using=db)
                except:
                    if was_exception:
                        # We want to see the original exception so let's
                        # just log the one after that.
                        log.exception(
                            'While trying to recover from an exception')
                    else:
                        raise
        # Force usage of our Task subclass.
        kw['base'] = Task
        # Default to ignoring results.
        if 'ignore_result' not in kw:
            kw['ignore_result'] = True
        return celery.task.task(**kw)(wrapped)
    if args:
        return decorate(*args)
    else:
        return decorate


@task_failure.connect
def process_failure_signal(exception, traceback, sender, task_id,
                           signal, args, kwargs, einfo, **kw):
    exc_info = (type(exception), exception, traceback)
    log.error(
        u'Celery TASK exception: %s: %s'
        % (exc_info[1].__class__.__name__, exc_info[1]),
        exc_info=exc_info,
        extra={
            'data': {
                'task_id': task_id,
                'sender': sender,
                'args': args,
                'kwargs': kwargs
            }
        })


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

