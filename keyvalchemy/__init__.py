"""
keyvalchemy

Simple dictionary like key value store - using SQLAlchemy to support multiple
database backends and pickle to store objects

"""
__title__ = 'keyvalchemy'
__version__ = '0.0.1'
__author__ = 'Chris Green'

from .keyvalchemy import KeyValchemy, ClosedKeyValchemy, ConnectionClosedError

def open(db_url, protocol=3, engine_kwargs=None):
    return KeyValchemy(db_url, protocol, engine_kwargs)
