# Mongobject

## About
**Mongobject** is a easy and useful library, which is based in Pymongo (), that lets you to use MongoDB in your Python code. It includes the connection configuration and the basic CRUD operations.

## Support
If you have a problem with the library or want to suggest something for improving the library, you can contact me by my Github account or via email (albertosml@correo.ugr.es).

## Documentation

This library includes the next operations:

- get_info(): Returns object information
- set_instance(uri=None, host=None, port=None): Modifies MongoDB instance

## Example
```
import Mongobject

m = Mongobject(host='localhost', port=27017, database='db', collection='col')

# Check for available collections
print(m.get_available_collections()) # ['col']

# Find
success = m.insert({ 'a': 1, 'b': 'hola' })

if success:
	print "Data inserted"
else:
	print "Data not inserted"
```
