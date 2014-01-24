GTFS importer
=============

Imports or updates a set of gtfs feeds to postgres. Adds utility functions to
database.


clear.rb
--------

Run this first. Clears all GTFS data from the database and creates the schema.


import\_gtfs.rb
--------------

Imports a single locally stored feed.


update\_feeds.rb
----------------

Designed to run periodically and check feeds online. Updates local database
when new versions are available.


feed\_dlds.yaml
-----------------

Created by `update_feeds.rb`, keeps track of updates. Can be edited to add
feeds. When not found, a default set is written.

