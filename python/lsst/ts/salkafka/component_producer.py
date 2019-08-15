# This file is part of ts_salkafka.
#
# Developed for the LSST Telescope and Site Systems.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

__all__ = ["ComponentProducer"]

import asyncio

import confluent_kafka.admin

from lsst.ts import salobj
from .topic_producer import TopicProducer


class ComponentProducer:
    """Produce Kafka messages from DDS samples for one SAL component.

    Parameters
    ----------
    domain : `lsst.ts.salobj.Domain`
        DDS domain participant and quality of service information.
    name : `str`
        Name of SAL component, e.g. "ATDome".
    schema_registry : `kafkit.registry.sansio.RegistryApi`
        A client for the Confluent registry of Avro schemas.
    broker_client : `confluent_kafka.admin.AdminClient`
        Kafka broker administration client.
    broker_url : `str`
        URL for Kafka broker.
    paritions : `int`
        Number of partitions for each Kafka topic.
    replication_factor : `int`
        Number of replicas for each Kafka partition.
    wait_for_ack : `int`
        0: do not wait (unsafe)
        1: wait for first kafka broker to respond (recommended)
        2: wait for all kafka brokers to respond
    log : `logging.Logger`
        Parent log.
    """
    def __init__(self, domain, name, schema_registry, broker_client,
                 broker_url, partitions, replication_factor, wait_for_ack, log):
        self.domain = domain
        # index=0 means we get samples from all SAL indices of the component
        self.salinfo = salobj.SalInfo(domain=self.domain, name=name, index=0)
        self._broker_client = broker_client
        self._schema_registry = schema_registry
        self._broker_url = broker_url
        self._partitions = partitions
        self._replication_factor = replication_factor
        self._wait_for_ack = wait_for_ack
        self.log = log.getChild(name)
        self.producers = set()

        # create a list of topic names and sal prefixes
        topic_name_prefixes = [("ackcmd", "")]
        topic_name_prefixes += [(cmd_name, "command_") for cmd_name in self.salinfo.command_names]
        topic_name_prefixes += [(evt_name, "logevent_") for evt_name in self.salinfo.event_names]
        topic_name_prefixes += [(tel_name, "") for tel_name in self.salinfo.telemetry_names]
        kafka_topic_names = [f"lsst.sal.{self.salinfo.name}.{prefix}{name}"
                             for name, prefix in topic_name_prefixes]

        self.log.info(f"Create Kafka topics for {self.salinfo} if not already present.")
        self.create_kafka_topics(kafka_topic_names)

        self.log.info(f"Create SAL/Kafka topic producers for {self.salinfo}.")
        try:
            for topic_name, sal_prefix in topic_name_prefixes:
                self._make_topic(name=topic_name, sal_prefix=sal_prefix)
            self.start_task = asyncio.ensure_future(self.start())
        except Exception:
            asyncio.ensure_future(self.salinfo.close())
            raise

    def create_kafka_topics(self, topic_names):
        """Create Kafka topics that do not already exist.
        """
        metadata = self._broker_client.list_topics(timeout=10)
        existing_topic_names = set(metadata.topics.keys())
        new_topic_names = set(topic_names) - existing_topic_names
        if len(new_topic_names) == 0:
            return
        new_topic_metadata = [confluent_kafka.admin.NewTopic(topic_name,
                                                             num_partitions=self._partitions,
                                                             replication_factor=self._replication_factor)
                              for topic_name in sorted(new_topic_names)]
        fs = self._broker_client.create_topics(new_topic_metadata)
        for topic_name, future in fs.items():
            try:
                future.result()  # The result itself is None
                self.log.info(f"Created {self.salinfo} topic {topic_name}")
            except Exception:
                self.log.exception(f"Failed to create {self.salinfo} topic {topic_name}")
                raise

    def _make_topic(self, name, sal_prefix):
        r"""Make a salobj read topic and associated topic producer.

        Parameters
        ----------
        name : `str`
            Topic name, without a "command\_" or "logevent\_" prefix.
        sal_prefix : `str`
            SAL topic prefix: one of "command\_", "logevent\_" or ""
        """
        topic = salobj.topics.ReadTopic(salinfo=self.salinfo,
                                        name=name,
                                        sal_prefix=sal_prefix,
                                        max_history=0)
        producer = TopicProducer(topic=topic,
                                 schema_registry=self._schema_registry,
                                 broker_url=self._broker_url,
                                 wait_for_ack=self._wait_for_ack,
                                 log=self.log)
        self.producers.add(producer)

    async def start(self):
        """Start the SalInfo and producers.
        """
        self.log.debug("starting")
        await self.salinfo.start()
        await asyncio.gather(*[producer.start_task for producer in self.producers])
        self.log.debug("started")

    async def close(self):
        """Shut down and clean up resources.

        Close the contained `SalInfo`, but not the `Domain`,
        because that may be used by other objects.
        """
        self.log.debug("close")
        await self.salinfo.close()
        await asyncio.gather(*[producer.close() for producer in self.producers])

    async def __aenter__(self):
        await self.start_task
        return self

    async def __aexit__(self, type, value, traceback):
        await self.close()
