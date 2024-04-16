import json

from sqlalchemy import TypeDecorator, TEXT


class StringListType(TypeDecorator):
    """TypeDecorator that lets us transform an array into a text column.  Works with both SQLite and Postgresql and is
    also simple and easy to use (unlike the built-in JSON querying mechanism)
    """
    impl = TEXT

    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None and type(value) is list or type(value) is dict:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value

    def copy(self, **kw):
        return StringListType(self.impl.length)
