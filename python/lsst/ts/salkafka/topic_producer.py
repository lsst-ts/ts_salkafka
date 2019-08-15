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

__all__ = ["TopicProducer"]

import asyncio
import time

import aiokafka
import kafkit.registry.serializer

from lsst.ts import salobj
from .make_avro_schema import make_avro_schema

# translate from wait_for_ack integer values to
# AIOKafkaProducer ack argument values
_WAIT_FOR_ACK_DICT = {
    0: 0,
    1: 1,
    2: "all",
}


class TopicProducer:
    """Produce Kafka messages from DDS samples for one topic.

    Parameters
    ----------
    topic : `salobj.topics.ReadTopic`
        Topic for which to produce kafka messages.
    schema_registry : `kafkit.registry.sansio.RegistryApi`
        A client for the Confluent registry of Avro schemas.
    broker_url : `str`
        URL for Kafka broker.
    wait_for_ack : `int`
        0: do not wait (unsafe)
        1: wait for first kafka broker to respond (recommended)
        2: wait for all kafka brokers to respond
    """
    def __init__(self, schema_registry, broker_url, wait_for_ack, topic, log):
        self._schema_registry = schema_registry
        self.topic = topic
        self.log = log.getChild(topic.sal_name)
        self._broker_url = broker_url
        self._wait_for_ack = _WAIT_FOR_ACK_DICT[wait_for_ack]
        self._avro_schema = make_avro_schema(topic)
        self._producer = None
        self.start_task = asyncio.ensure_future(self.start())

    async def close(self):
        if self._producer is not None:
            self.log.debug("close producer")
            await self._producer.stop()

    async def start(self):
        """Get the schema and connect the callback function.
        """
        self.log.debug("starting")
        serializer = await kafkit.registry.serializer.Serializer.register(
            registry=self._schema_registry,
            schema=self._avro_schema,
            subject=f"{self._avro_schema['name']}-value",
        )
        self._producer = aiokafka.AIOKafkaProducer(
            loop=asyncio.get_running_loop(),
            bootstrap_servers=self._broker_url,
            acks=self._wait_for_ack,
            value_serializer=serializer,
        )
        await self._producer.start()
        self.topic.callback = self
        self.log.debug("started")

    async def __call__(self, data):
        """Forward one sample from DDS to Kafka.

        Parameters
        ----------
        data : ``any``
            DDS sample.
        """
        avro_data = data.get_vars()
        avro_data["private_kafkaStamp"] = salobj.tai_from_utc(time.time())
        await self._producer.send_and_wait(self._avro_schema["name"], value=avro_data)
