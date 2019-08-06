.. py:currentmodule:: lsst.ts.salkafka

.. _lsst.ts.salkafka:

################
lsst.ts.salkafka
################

Forward samples from SAL/DDS to Kafka to populate engineering facilities databases.

.. .. _lsst.ts.salkafka-using:

Using lsst.ts.salkafka
======================

Run command-line script ``run_salkafka_producer.py`` to forward DDS samples to Kafka for the specified SAL components.
Run ``run_salkafka_producer.py --help`` for information about the arguments.
The script logs to stderr.

To stop a producer, terminate its process or use ctrl-C.

It is perfectly reasonable to have one producer for many SAL components as long as none of them is very chatty.
For chatty components such as ``M1M3`` and ``ATMCS`` it is safer to use a single producer.

Requirements
============

Third party packages (all pip-installable):

* aiohttp
* aiokafka
* kafkit

LSST packages:

* ts_salobj
* ts_idl
* Built IDl files for all components you want to monitor.

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
