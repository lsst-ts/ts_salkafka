.. py:currentmodule:: lsst.ts.salkafka

.. _lsst.ts.salkafka:

################
lsst.ts.salkafka
################

Forward DDS samples from one or more SAL components to a Kafka broker, to populate databases.

.. .. _lsst.ts.salkafka-using:

Using lsst.ts.salkafka
======================

Run command-line script ``run_salkafka_producer.py`` to forward DDS samples from the specified SAL comonents to Kafka.
Run with ``--help`` for information about the arguments.
The script logs to stderr.
You may run as many instances as you like.

To stop a producer, terminate its process or use ctrl-C.

Until performance is tested, my best guess is that one producer should forward no more than 1000 messages per second, on average.
This is based on the fact that ts_salobj can send and receive roughly 5000 topics/second using one process on a modern iMac.
This suggests that it should be fine to use one producer for many SAL components as long as none of them is very chatty.

Requirements
============

Third party packages (all pip-installable):

* aiohttp
* aiokafka
* confluent-kafka
* kafkit

LSST packages:

* ts_salobj
* ts_idl
* Built IDL files for all components you want to monitor.

.. _lsst.ts.salkafka-contributing:

Contributing
============

``lsst.ts.salkafka`` is developed at https://github.com/lsst-ts/ts_salkafka.
You can find Jira issues for this module under the `ts_salkafka <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20component%20%3D%20ts_salkafka>`_ component.

Python API reference
====================

.. automodapi:: lsst.ts.salkafka
   :no-main-docstr:
   :no-inheritance-diagram:
