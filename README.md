CBBigFile
=========

A crude way to store and retrieve big files in Couchbase.

What's the difference with cbfs ?
---------------------------------

cbfs is the "official" way to store huge files in Couchbase. It's available here : https://github.com/couchbaselabs/cbfs

Compared to cbfs, cbbigfile is a lot less advanced, but as it stores everything
as documents in a bucket, it's fully compatible with XDCR for instance (cbfs
actually only uses Couchbase for metadata and cluster topology, and stores
actual file data directly onto the nodes).
