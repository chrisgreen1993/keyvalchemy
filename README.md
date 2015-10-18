# keyvalchemy

Simple key value store that uses SQLAlchemy to support multiple backends and pickle
to dump and load objects to and from the database.

Works just like a normal dict

Example usage:

```python
    import keyvalchemy
    with keyvalchemy.open('sqlite:///:memory:') as db:
        db['a_key'] = [1,2,3] # Store at key 'a_key'
        list = db['a_key'] # Get the list back
        list.append(4) # Append
        db['a_key'] = list # And store back to db
        del db['a_key'] # Delete the key
    # db is closed automaically in a with statement
```

To install, clone repo, then run `python setup.py install` from repo directory (not on pypi yet)

To run tests, run `python -m unittest discover` from repo directory

Currently only tested with python 3.4
