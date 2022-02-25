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

import asyncio
import contextlib
import logging
import unittest

import numpy as np

from lsst.ts import salobj
from lsst.ts import salkafka
from lsst.ts import utils

np.random.seed(47)


class TopicProducerTestCase(unittest.IsolatedAsyncioTestCase):
    def run(self, result=None):
        """Override `run` to insert mocks for every test.

        https://stackoverflow.com/a/11180583
        """
        salobj.set_random_lsst_dds_partition_prefix()
        with salkafka.mocks.insert_all_mocks(), utils.modify_environ(LSST_SITE="test"):
            super().run(result)

    @contextlib.asynccontextmanager
    async def make_producer(self, topic_name, sal_prefix):
        """Make a CSC and topic producer for a specified topic of the
        Test SAL component.

        Parameters
        ----------
        topic_name : `str`
            Topic name, without any prefix.
            For example specify summaryState for logevent_summaryState.
        sal_prefix : `str`
            SAL prefix for the topic.
            For example specify logevent_ for logevent_summaryState.

        Attributes
        ----------
        csc : `lsst.ts.salobj.TestCsc`
            A Test CSC you can use to produce data.
        topic_producer : `TopicProducer`
            The topic producer.
        """

        # Use an arbitrary non-zero index for the producer to test that
        # the topic producer can see it.
        self.csc = salobj.TestCsc(index=5)

        # Always use an index of 0 for the TopicProducer's read topic
        # (which we get from this SalInfo).
        read_salinfo = salobj.SalInfo(domain=self.csc.domain, name="Test", index=0)

        log = logging.getLogger()
        log.addHandler(logging.StreamHandler())
        log.setLevel(logging.INFO)

        kafka_config = salkafka.KafkaConfiguration(
            broker_url="test.kafka:9000",
            registry_url="https://registry.test.kafka/",
            partitions=2,
            replication_factor=3,
            wait_for_ack=1,
        )
        kafka_factory = salkafka.KafkaProducerFactory(
            config=kafka_config,
            log=log,
        )

        # We are not sure we can construct the topic producer
        # because the topic data may be invalid),
        # so initialize it to None for proper cleanup.
        self.topic_producer = None
        try:
            read_topic = salobj.topics.ReadTopic(
                salinfo=read_salinfo,
                name=topic_name,
                sal_prefix=sal_prefix,
                max_history=0,
                filter_ackcmd=False,
            )

            self.topic_producer = salkafka.TopicProducer(
                kafka_factory=kafka_factory, topic=read_topic, log=log
            )
            await asyncio.gather(
                self.topic_producer.start_task,
                kafka_factory.start_task,
                read_salinfo.start(),
                self.csc.start_task,
            )

            # Run the unit test
            yield
        finally:
            if self.topic_producer is not None:
                await self.topic_producer.close()
            await kafka_factory.close()
            await read_salinfo.close()
            await self.csc.close()

    async def test_basics(self):
        async with self.make_producer(topic_name="arrays", sal_prefix="logevent_"):
            for isample in range(3):
                arrays_dict = self.csc.make_random_arrays_dict()
                await self.csc.evt_arrays.set_write(**arrays_dict)
                for iread in range(10):
                    if len(self.topic_producer.kafka_producer.sent_data) > isample:
                        break
                    await asyncio.sleep(0.01)
                else:
                    self.fail("Data not seen in time")
                assert len(self.topic_producer.kafka_producer.sent_data) == isample + 1
                (
                    kafka_topic_name,
                    sent_value,
                    serialized_value,
                ) = self.topic_producer.kafka_producer.sent_data[-1]
                assert kafka_topic_name == "lsst.sal.Test.logevent_arrays"
                assert isinstance(serialized_value, bytes)
                for key, value in arrays_dict.items():
                    if key == "private_rcvStamp":
                        # not set in arrays_dict but set in received
                        # sample and thus in ``sent_value``
                        continue
                    if isinstance(value, np.ndarray):
                        np.testing.assert_array_equal(sent_value[key], value)
                    else:
                        assert sent_value[key] == value

    async def test_ackcmd(self):
        """ackcmd topics are special, so make sure they work.

        This exercises DM-25707
        """
        async with self.make_producer(topic_name="ackcmd", sal_prefix=""):
            for isample in range(3):
                await self.csc.salinfo._ackcmd_writer.set_write(private_seqNum=isample)
                for iread in range(10):
                    if len(self.topic_producer.kafka_producer.sent_data) > isample:
                        break
                    await asyncio.sleep(0.01)
                else:
                    self.fail("Data not seen in time")
                assert len(self.topic_producer.kafka_producer.sent_data) == isample + 1
                (
                    kafka_topic_name,
                    sent_value,
                    serialized_value,
                ) = self.topic_producer.kafka_producer.sent_data[-1]
                assert kafka_topic_name == "lsst.sal.Test.ackcmd"
                assert isinstance(serialized_value, bytes)
                assert sent_value["private_seqNum"] == isample
