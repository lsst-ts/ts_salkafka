.. py:currentmodule:: lsst.ts.salkafka

.. _lsst.ts.salkafka.version_history:

###############
Version History
###############

v1.11.0
-------

* Add support for SASL authentication using SASL_PLAINTEXT protocol and SCRAM-SHA-512 method.
  This adds the ``--username`` and ``--password`` CLI options that can be used to
  connect the secured Kafka broker.

v1.10.1
-------

* Remove "test" section from "conda/meta.yaml" again because it depends on non-conda dependencies.

v1.10.0
-------

* Rename command-line scripts to remove ".py" suffix.
* Update for ts_sal 7, which is required:

  * Remove all references to the "priority" field (RFC-848).
  * Rename "{component_name}ID" fields to "salIndex" (RFC-849).

* Add support for standard Avro schema evolution by specifying a default value for each field.
  This adds automatic schema evolution support for adding and removing fields, but does not support changing the type of an existing field.
  Use a default value of 0 for float fields and float array items, because that matches the default for Kafka and DDS.
* Build with pyproject.toml.
* Fix the continuous integration Jenkinsfile to build the missing Script IDL component and modernize the content.

v1.9.0
------

* Update for ts_salobj 7, which is required.
* Modernize the ``Jenkinsfile``.

v1.8.0
------

* Allow specifying the number of Kafka partitions per topic set (e.g. when running a producer with the ``--file`` argument).
* `TopicNames`: make it a `dataclasses.dataclass` and add a ``partition`` field.
* `TopicNamesSet`: add ``default_partition`` constructor argument.
  Update the schema to require at least one topic set.
* `KafkaConfiguration`: make it a `dataclasses.dataclass`.
* `ComponentProducerSet.create_producer_subprocess`: set Kafka partitions from `TopicNames`.
* Update to use ts_utils.
* Update to use pytest-black.
* Modernize unit tests to use bare assert.

v1.7.1
------

* Fix semaphore file for partial producer mode.

v1.7.0
------

* Add ``private_efdStamp`` field to Avro schema.
  Set it to ``private_sndStamp`` converted to UTC unix seconds.
* Added some timeouts to reduce the danger of infinite hangs.

v1.6.0
------

* Implement new feature that allows dividing the topics for a SAL component into multiple subprocesses,
  so the producer can keep up with a chatty component such as MTM1M3.
* Modernize the documentation.
* Add ``test_component_producer_set.py``.
* Use ``pre-commit``; see the README.rst for instructions.
* Update tests to use `unittest.IsolatedAsyncioTestCase` instead of the abandoned asynctest package.
* Fix setting up version on conda package.
* Fix upload command in Jenkinsfile.conda.
* Format the code with black 20.8b1.

v1.5.0
------

* Add semaphore file for Kubernetes startup probe.

v1.4.0
------

* Make SAL/Kafka producers shut down gracefully.
* Update the pre-commit hook to block the commit if any code is not formatted with black.

v1.3.0
------

* Fix handling of ackcmd data. The fix requires ts_salobj 5.16 or 6

v1.2.0
------

* Update schema test for compatibility with ts_sal 4-5.
  ts_sal 4.2 will add the private_identity field and ts_sal 5 will remove the private_host field.

v1.1.3
------

* Added a test that code is formatted with black.
* Removed ``sudo: false`` from ``.travis.yml``.

v1.1.2
------

* Code formatted by ``black``, with a pre-commit hook to enforce this. See the README file for configuration instructions.
* Fix the ``Contributing`` entry in ``index.rst``.
* Added a revision history.

v1.1.1
------

* Add conda build support.

v1.1.0
------

* Add topic metadata to the Avro schema.
  For the most complete metadata build your IDL files using ts_sal 4.1,
  because it provides more metadata than ts_sal 4.0.

v1.0.1
------

* Update unit tests to handle missing char0 field in arrays topics.

v1.0.0
------

* First release.
