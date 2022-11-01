.. py:currentmodule:: lsst.ts.salkafka

.. _lsst.ts.salkafka:

################
lsst.ts.salkafka
################

.. image:: https://img.shields.io/badge/GitHub-gray.svg
    :target: https://github.com/lsst-ts/ts_salkafka
.. image:: https://img.shields.io/badge/Jira-gray.svg
    :target: https://jira.lsstcorp.org/issues/?jql=labels+%3D+ts_salkafka

.. _lsst.ts.salkafka-overview:

Overview
========

ts_salkafka is a service to monitor SAL/DDS data from SAL components and forward it to a Kafka broker.
Other code reads the Kafka messages from the broker and populates the engineering facilities database (EFD).

One SAL/Kafka producer service can monitor data for multiple SAL components, as long as they are not too chatty.
For very chatty SAL components, such as MTM1M3, it is best to run a single producer
with topics divided among subprocesses, using the ``--file`` option described below.

.. _lsst.ts.salkafka-user_guide:

User Guide
==========

Use command-line script ``run_salkafka_producer.py`` to run a producer service that forwards DDS samples from specified SAL comonents to Kafka.
There are two examples below.
Run with ``--help`` for information about the arguments.
Producers log to stderr.

To stop a producer, terminate its process with SIGTERM (not SIGKILL) or use ctrl-C.

For most SAL components it is reasonable to use one producer to handle multiple components.
Here is an example (with nonsense kafka arguments)::

    run_salkafka_producer.py MTDome MTDomeTrajectory --broker test.kafka:9000 --registry https://registry.test.kafka/

For a few chatty components, such as MTM1M3, you must dedicate one producer to that component,
splitting its topics among subprocesses by using the ``--file`` argument
with a configuration file that specifies how to divide the topics.
Here is an example (with nonsense kafka arguments and arbitary numbers of partitions in the yaml file)::

    run_salkafka_producer.py --file MTM1M3.yaml --broker test.kafka:9000 --registry https://registry.test.kafka/

where MTM1M3.yaml contains::

    component: MTM1M3
    queue_len: 1000
    topic_sets:
    - events:
        - logMessage
    - events:
        - hardpointActuatorWarning
    - events:
        - hardpointMonitorWarning
    - telemetry:
        - appliedCylinderForces
    - telemetry:
        - forceActuatorData
    - telemetry:
        - inclinometerData
    - telemetry:
        - outerLoopData
    - telemetry:
        - accelerometerData
    - telemetry:
        - hardpointActuatorData
    - telemetry:
        - imsData
    - telemetry:
        - gyroData
    - telemetry:
        - powerSupplyData
    - telemetry:
        - pidData
    - telemetry:
        - hardpointMonitorData

You will be able to tell if you ask a producer to do too much,
because you will see log messages about falling behind,
possibly followed by log messages about losing data.

Developer Guide
===============

.. toctree::
    developer_guide
    :maxdepth: 1

Version History
===============

.. toctree::
    version_history
    :maxdepth: 1
