"""
A monkeypatch for ``django.dispatch`` to send signals asynchronously.

Usage::

    >>> import async_signals
    >>> async_signals.start_the_machine()

``django.dispatch.Signal.send`` is replaced with an asynchronous version that
adds the signal to a queue processed on a background thread.  The synchronous
``Signal.send`` is still available as ``Signal.sync_send``.

"""
import functools
import logging
import Queue
import threading

from django.dispatch import Signal
from django.db import models

log = logging.getLogger('signals')
_signal_queue = Queue.Queue()


@functools.wraps(Signal.send)
def async_send(self, sender, **named):
    # Bail early if no one is listening.
    if not self.receivers:
        return []
    # Django depends on class_prepared being synchronous.
    if self is models.signals.class_prepared:
        return Signal.sync_send(self, sender, **named)
    _signal_queue.put_nowait((self, sender, named))
    return []


def listener():
    while 1:
        # Make sure this thread can't die.
        try:
            try:
                self, sender, kw = _signal_queue.get()
            except ValueError:
                log.error('Error unpacking queue item.', exc_info=True)
            try:
                Signal.sync_send(self, sender, **kw)
            except Exception:
                log.error('Error calling signal.', exc_info=True)
        except:
            log.critical('Uncaught error.', exc_info=True)


def start_the_machine():
    # Monkeypatch!
    Signal.sync_send = Signal.send
    Signal.send = async_send
    # Start the listener.
    thread = threading.Thread(target=listener)
    thread.daemon = True
    thread.start()
