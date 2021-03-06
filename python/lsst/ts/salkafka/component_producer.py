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
    kafka_info : `KafkaInfo`
        Information and clients for using Kafka.
    """

    def __init__(self, domain, name, kafka_info):
        self.domain = domain
        # index=0 means we get samples from all SAL indices of the component
        self.salinfo = salobj.SalInfo(domain=self.domain, name=name, index=0)
        self.kafka_info = kafka_info
        self.log = kafka_info.log.getChild(name)
        self.topic_producers = dict()
        """Dict of topic attr_name: TopicProducer.
        """

        # Create a list of (basic topic name, SAL topic name prefix).
        topic_name_prefixes = [("ackcmd", "")]
        topic_name_prefixes += [
            (cmd_name, "command_") for cmd_name in self.salinfo.command_names
        ]
        topic_name_prefixes += [
            (evt_name, "logevent_") for evt_name in self.salinfo.event_names
        ]
        topic_name_prefixes += [
            (tel_name, "") for tel_name in self.salinfo.telemetry_names
        ]
        kafka_topic_names = [
            f"lsst.sal.{self.salinfo.name}.{prefix}{name}"
            for name, prefix in topic_name_prefixes
        ]

        self.log.info(
            f"Creating Kafka topics for {self.salinfo.name} if not already present."
        )
        self.kafka_info.make_kafka_topics(kafka_topic_names)

        self.log.info(f"Creating SAL/Kafka topic producers for {self.salinfo.name}.")
        try:
            for topic_name, sal_prefix in topic_name_prefixes:
                self._make_topic(name=topic_name, sal_prefix=sal_prefix)
            self.start_task = asyncio.ensure_future(self.start())
        except Exception:
            asyncio.ensure_future(self.salinfo.close())
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
        topic = salobj.topics.ReadTopic(
            salinfo=self.salinfo,
            name=name,
            sal_prefix=sal_prefix,
            max_history=0,
            filter_ackcmd=False,
        )
        producer = TopicProducer(topic=topic, kafka_info=self.kafka_info, log=self.log)
        self.topic_producers[topic.attr_name] = producer

    async def start(self):
        """Start the contained `lsst.ts.salobj.SalInfo` and Kafka producers.
        """
        self.log.debug("starting")
        await self.salinfo.start()
        await asyncio.gather(
            *[producer.start_task for producer in self.topic_producers.values()]
        )
        self.log.debug("started")

    async def close(self):
        """Shut down and clean up resources.

        Close the contained `lsst.ts.salobj.SalInfo`, but not the ``domain``,
        because that is almost certainly used by other objects.
        """
        self.log.debug("closing")
        await self.salinfo.close()
        await asyncio.gather(
            *[producer.close() for producer in self.topic_producers.values()]
        )

    async def __aenter__(self):
        await self.start_task
        return self

    async def __aexit__(self, type, value, traceback):
        await self.close()
