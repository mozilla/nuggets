import logging
from functools import wraps

from django.conf import settings

import commonware
from pyes import ES

_es = None
log = logging.getLogger('elasticsearch')


def get_es():
    """Return one es object."""
    global _es
    if not _es:
        _es = ES(settings.ES_HOSTS, default_indexes=[settings.ES_INDEX])
    return _es


def es_required(f):
    @wraps(f)
    def wrapper(*args, **kw):
        if settings.ES_DISABLED:
            log.warning('Search not available for %s.' % f)
            return

        return f(*args, es=get_es(), **kw)
    return wrapper
