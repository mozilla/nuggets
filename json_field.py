import json

from django.db import models
from django.core.serializers.json import DjangoJSONEncoder


# https://bitbucket.org/offline/django-annoying
class JSONField(models.TextField):
    """
    JSONField is a generic textfield that neatly serializes/unserializes
    JSON objects seamlessly.
    Django snippet #1478

    example:
        class Page(models.Model):
            data = JSONField(blank=True, null=True)


        page = Page.objects.get(pk=5)
        page.data = {'title': 'test', 'type': 3}
        page.save()
    """

    __metaclass__ = models.SubfieldBase

    def to_python(self, value):
        if value == '':
            return None

        try:
            if isinstance(value, basestring):
                return json.loads(value)
        except ValueError:
            pass
        return value

    def get_db_prep_save(self, value, *args, **kwargs):
        if value == '':
            return None
        value = json.dumps(value, cls=DjangoJSONEncoder, separators=(',', ':'))
        return super(JSONField, self).get_db_prep_save(value, *args, **kwargs)


# South support.
try:
    from south.modelsinspector import add_introspection_rules
except ImportError:
    pass
else:
    add_introspection_rules([], ['^json_field\.JSONField'])
