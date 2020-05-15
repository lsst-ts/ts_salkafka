.. py:currentmodule:: lsst.ts.salkafka

.. _lsst.ts.salkafka.version_history:

###############
Version History
###############

v1.1.3
======

Changes:

* Added a test that code is formatted with black.
* Removed ``sudo: false`` from ``.travis.yml``.

Requirements:

* ts_salobj 5.11
* ts_idl 1
* IDL files for all CSCs
* aiohttp 3.5
* aiokafka 0.5
* confluent-kafka v1.1
* kafkit 0.1

v1.1.2
======

Changes:

* Code formatted by ``black``, with a pre-commit hook to enforce this. See the README file for configuration instructions.
* Fix the ``Contributing`` entry in ``index.rst``.
* Added a revision history.

Requirements:

* ts_salobj 5.11
* ts_idl 1
* IDL files for all CSCs
* aiohttp 3.5
* aiokafka 0.5
* confluent-kafka v1.1
* kafkit 0.1

v1.1.1
======

Add conda build support.

Requirements:

* ts_salobj 5.11
* ts_idl 1
* IDL files for all CSCs
* aiohttp 3.5
* aiokafka 0.5
* confluent-kafka v1.1
* kafkit 0.1


v1.1.0
======

Add topic metadata to the Avro schema.
For the most complete metadata build your IDL files using ts_sal 4.1,
because it provides more metadata than ts_sal 4.0.

Requirements:

* ts_salobj 5.1
* ts_idl 1
* IDL files for all CSCs
* aiohttp 3.5
* aiokafka 0.5
* confluent-kafka v1.1
* kafkit 0.1

v1.0.1
======

Update unit tests to handle missing char0 field in arrays topics.

Requirements:

* ts_salobj 4.5
* ts_idl
* IDL files for all CSCs
* aiohttp 3.5
* aiokafka 0.5
* confluent-kafka v1.1
* kafkit 0.1


v1.0.0
======

First release.

Requirements:

* ts_salobj 4.5
* ts_idl
* IDL files for all CSCs
* aiohttp 3.5
* aiokafka 0.5
* confluent-kafka v1.1
* kafkit 0.1
