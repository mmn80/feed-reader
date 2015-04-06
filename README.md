Counterfeeder
=============

Multi-platform, multi-node RSS/Atom feed reader

Features / TODO
---------------

### DB

* acid-state based document DB
* sharding system for fast initial loading and constant memory operation
* all combined master+shard operations are ACID
* O(p*log(n)) queries, with p = result set size ('infinite scroll', etc.)

### Backend

* client/server architecture
* async downloading & processing of feeds
* nodes should also work on mobile (unlike tt-rss)
* p2p sync among server instances

### Frontend

* native mobile & web clients
* async UI rendering


License
-------
BSD style
