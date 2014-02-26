memstore
========

Concurrent in-memory key value store, that can be serialized to disk and offers journaling.

To put something in the store and serialize it:

	Memstore store = new Memstore("directory/data");
	store.put(someKey, someValue);
	store.serialize();
	
Then to get it out again:

	Object loaded = memstore.get(key);
