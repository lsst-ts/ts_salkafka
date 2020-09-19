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
import logging
import time

from multiprocessing import Process, Queue

from lsst.ts import salobj
from lsst.ts import salkafka
from .make_avro_schema import make_avro_schema


def make_kafka_producer(
    queue,
    component,
    prefix,
    name,
    topic,
    broker_url,
    registry_url,
    partitions,
    replication_factor,
    wait_for_ack,
    avro_schema,
):

    log = logging.getLogger()

    kafka_info = salkafka.KafkaInfo(
        broker_url=broker_url,
        registry_url=registry_url,
        partitions=partitions,
        replication_factor=replication_factor,
        wait_for_ack=wait_for_ack,
        log=log,
    )

    log.info(
        f"Creating Kafka topic lsst.sal.{component}.{prefix}{name} if not already present."
    )
    kafka_info.make_kafka_topics([f"lsst.sal.{component}.{prefix}{name}"])

    asyncio.set_event_loop(asyncio.new_event_loop())

    async def run():
        await kafka_info.start()
        kafka_producer = await kafka_info.make_producer(avro_schema=avro_schema)

        log.info("Running")
        while True:
            data = queue.get(block=True)
            await kafka_producer.send_and_wait(avro_schema["name"], value=data)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())


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

    def __init__(
        self,
        component,
        prefix,
        name,
        topic,
        log,
        broker_url,
        registry_url,
        partitions,
        replication_factor,
        wait_for_ack,
        max_queue=1000,
    ):
        self.topic = topic
        self.log = log.getChild(topic.sal_name)

        self.avro_schema = make_avro_schema(topic)

        self._data_queue = Queue()
        self._kafka_producer = Process(
            target=make_kafka_producer,
            daemon=True,
            args=(
                self._data_queue,
                component,
                prefix,
                name,
                topic,
                broker_url,
                registry_url,
                partitions,
                replication_factor,
                wait_for_ack,
                self.avro_schema,
            ),
        )
        self.start_task = asyncio.ensure_future(self.start())

        self.print_filling_up_warning = True
        self.print_queue_full_warning = True
        self.queue_full = False

        # When to star dropping messages
        self.full_level = max_queue

        # When to warn that send queue is filling
        self.warning_level = int(max_queue / 2)

        # When to clear warning
        self.clear_warning_level = int(max_queue / 4)

        # When to resume sending messages
        self.resume_level = max(self.clear_warning_level - 1, 2)

        self.discarded_samples = 0

        self.last_data_sent = None
        self.n_data_sent = 0

    async def close(self):
        """Close the Kafka producer.
        """
        if self._kafka_producer is not None:
            self.log.debug("Closing kafka producer")
            self._kafka_producer.kill()
            self._kafka_producer.join(timeout=10)

    async def start(self):
        """Start the Kafka producer.
        """
        self.log.debug("Making kafka producer")
        # self.kafka_producer = await self.kafka_info.make_producer(
        #     avro_schema=self.avro_schema
        # )
        self._kafka_producer.start()
        self.topic.callback = self

    async def __call__(self, data):
        """Forward one DDS sample (message) to Kafka.

        Parameters
        ----------
        data : ``dds sample``
            DDS sample.
        """

        list_length = self._data_queue.qsize()

        # store current value of flag
        queue_full = self.queue_full

        self.queue_full = (
            list_length > self.full_level
            if not self.queue_full
            else list_length > self.resume_level
        )

        if not self.queue_full:

            avro_data = data.get_vars()
            avro_data["private_kafkaStamp"] = salobj.tai_from_utc(time.time())

            self._data_queue.put_nowait(avro_data)
            self.last_data_sent = avro_data
            self.n_data_sent += 1

            if list_length > self.warning_level and self.print_filling_up_warning:
                self.print_filling_up_warning = False
                self.log.warning(
                    f"{self.topic.name}: Send and wait list filling up: {list_length}/{self.full_level} "
                )
            elif list_length < self.clear_warning_level:
                self.print_filling_up_warning = True
                self.print_queue_full_warning = True

            # This means we just transitioned from not writting to writting
            # data.
            if queue_full:
                self.log.info(
                    f"{self.topic.name}: Resume writting data: {data.private_seqNum}. "
                    f"Discarded {self.discarded_samples} samples."
                )
                self.discarded_samples = 0

        elif self.print_queue_full_warning:
            self.print_queue_full_warning = False
            self.discarded_samples += 1
            self.log.warning(
                f"{self.topic.name}: Send and wait list full. Discarding samples. "
                f"Starting at {data.private_seqNum}."
            )
        else:
            self.discarded_samples += 1
