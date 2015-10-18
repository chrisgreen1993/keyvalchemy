"""
keyvalchemy

Simple key value store that uses SQLAlchemy to support multiple backends and pickle
to dump and load objects to and from the database.

Works just like a normal dict

Example usage:

    import keyvalchemy
    with keyvalchemy.open('sqlite:///:memory:') as db:
        db['a_key'] = [1,2,3] # Store at key 'a_key'
        list = db['a_key'] # Get the list back
        list.append(4) # Append
        db['a_key'] = list # And store back to db
        del db['a_key'] # Delete the key
    # db is closed automaically in a with statement
"""
__title__ = 'keyvalchemy'
__version__ = '0.0.1'
__author__ = 'Chris Green'

from .keyvalchemy import KeyValchemy, ClosedKeyValchemy, ConnectionClosedError

def open(db_url, protocol=3, engine_kwargs=None):
    """
    Used to open connection to database
    Args:
        See KeyValchemy.__init__()
    Returns:
        KeyValchemy object
    """
    return KeyValchemy(db_url, protocol, engine_kwargs)
