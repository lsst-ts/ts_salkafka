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

from lsst.ts import salobj
from .make_avro_schema import make_avro_schema


class TopicProducer:
    """Produce Kafka messages from DDS samples for one topic.

    Parameters
    ----------
    topic : `salobj.topics.ReadTopic`
        Topic for which to produce kafka messages.
    kafka_info : `KafkaInfo`
        Information and clients for using Kafka.
    log : `logging.Logger`
        Parent log.
    """

    def __init__(self, topic, kafka_info, log, max_queue_size=10):
        self.topic = topic
        self.kafka_info = kafka_info
        self.log = log.getChild(topic.sal_name)
        self.kafka_producer = None
        self.avro_schema = make_avro_schema(topic)

        self.send_and_wait_task = asyncio.Future()
        self.send_and_wait_task.set_result(0)
        self.max_queue_size = max_queue_size
        self.discarded_samples = 0
        self.discard_timer_task = asyncio.Future()
        self.discard_timer_task.set_result(0)

        self.start_task = asyncio.ensure_future(self.start())

    async def close(self):
        """Close the Kafka producer.
        """
        if self.kafka_producer is not None:
            self.log.debug("Closing kafka producer")
            await self.kafka_producer.stop()

    async def start(self):
        """Start the Kafka producer.
        """
        self.log.debug("Making kafka producer")
        self.kafka_producer = await self.kafka_info.make_producer(
            avro_schema=self.avro_schema
        )
        self.topic.callback = self

    async def __call__(self, data):
        """Forward one DDS sample (message) to Kafka.

        Parameters
        ----------
        data : ``dds sample``
            DDS sample.
        """

        if (
            len(self.topic._data_queue) > self.max_queue_size
            and self.discard_timer_task.done()
        ):
            self.log.info(
                f"{self.topic.name} python thread filling up. Starting 1s data-write moratory."
            )
            self.discarded_samples += 1
            self.discard_timer_task = asyncio.create_task(asyncio.sleep(1))

        elif self.discard_timer_task.done():
            avro_data = data.get_vars()
            avro_data["private_kafkaStamp"] = salobj.tai_from_utc(time.time())

            await self.kafka_producer.send_and_wait(
                self.avro_schema["name"], value=avro_data
            )
            if self.discarded_samples > 0:
                self.log.info(
                    f"{self.topic.name}: Discarded {self.discarded_samples} samples."
                )
                self.discarded_samples = 0
        else:
            self.discarded_samples += 1
