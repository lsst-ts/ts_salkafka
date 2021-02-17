.. py:currentmodule:: lsst.ts.salkafka

.. _lsst.ts.salkafka-developer_guide:

###############
Developer Guide
###############

ts_salkafka is implemented using ts_salobj, aiokafka, confluent-kafka and kafkit.

.. _lsst.ts.salkafka-api:

API
===

The primary classes are:

* `ComponentProducerSet`: monitor one or more SAL components and produce Kafka messages for all data received.
  This is the class that provides the command-line interface.
* `ComponentProducer`: monitor one SAL component (all topics or possibly a subset) and produce Kafka messages for all data received.

.. automodapi:: lsst.ts.salkafka
   :no-main-docstr:
   :no-inheritance-diagram:

.. _lsst.ts.salkafka-build:

Build and Test
==============

This is a conda-installable pure python package.
Once you have installed the package there is nothing to build except the documentation.

.. code-block:: bash

    make_idl_files.py Test
    setup -r .
    pytest -v  # to run tests
    package-docs clean; package-docs build  # to build the documentation

Requirements
------------

* Packages listed in ``conda/meta.yaml`` and ``setup.py``.
* Built IDL files for ``Test`` (for unit tests), plus all components you want to monitor.

.. _lsst.ts.salkafka-contributing:

Contributing
============

``lsst.ts.salkafka`` is developed at https://github.com/lsst-ts/ts_salkafka.
You can find Jira issues for this module using `labels=ts_salkafka <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20labels%20%20%3D%20ts_salkafka>`_.
