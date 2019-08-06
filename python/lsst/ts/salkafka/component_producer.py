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
    schema_registry : `kafkit.registry.sansio.RegistryApi`
        A client for the Confluent registry of Avro schemas.
    broker_url : `str`
        URL for Kafka broker.
    wait_for_ack : `int`
        0: do not wait (unsafe)
        1: wait for first kafka broker to respond (recommended)
        2: wait for all kafka brokers to respond
    log : `logging.Logger`
        Parent log.
    """
    def __init__(self, domain, name, schema_registry, broker_url, wait_for_ack, log):
        self.domain = domain
        # index=0 means we get samples from all SAL indices of the component
        self.salinfo = salobj.SalInfo(domain=self.domain, name=name, index=0)
        self._schema_registry = schema_registry
        self._broker_url = broker_url
        self._wait_for_ack = wait_for_ack
        self.log = log.getChild(name)
        self.producers = set()
        self.log.debug("creating topic producers")
        try:
            for cmd_name in self.salinfo.command_names:
                self._make_topic(name=cmd_name, sal_prefix="command_")
            for evt_name in self.salinfo.event_names:
                self._make_topic(name=evt_name, sal_prefix="logevent_")
            for tel_name in self.salinfo.telemetry_names:
                self._make_topic(name=tel_name, sal_prefix="")
            self._make_topic(name="ackcmd", sal_prefix="")

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
