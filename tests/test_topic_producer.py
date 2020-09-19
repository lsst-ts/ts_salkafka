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
import logging
import unittest

import asynctest
import numpy as np

from lsst.ts import salobj
from lsst.ts import salkafka

np.random.seed(47)


class TopicProducerTestCase(asynctest.TestCase):
    def run(self, result=None):
        """Override `run` to insert mocks for every test.

        https://stackoverflow.com/a/11180583
        """
        with salkafka.mocks.insert_all_mocks():
            super().run(result)

    def setUp(self):
        salobj.set_random_lsst_dds_domain()
        self.csc = None
        self.remote = None
        self.kafka_info = None
        self.producer = None

    async def tearDown(self):
        await asyncio.gather(
            self.topic_producer.close(),
            self.kafka_info.close(),
            self.read_salinfo.close(),
            self.csc.close(),
        )

    async def make_producer(self, topic_name, sal_prefix):

        # Use a non-zero index for the producer to test that
        # the topic producer can see it.
        self.csc = salobj.TestCsc(index=5)

        # Always use an index of 0 for the TopicProducer's read topic
        # (which we get from the remote).
        self.read_salinfo = salobj.SalInfo(domain=self.csc.domain, name="Test", index=0)
        read_topic = salobj.topics.ReadTopic(
            salinfo=self.read_salinfo,
            name=topic_name,
            sal_prefix=sal_prefix,
            max_history=0,
            filter_ackcmd=False,
        )
        log = logging.getLogger()
        log.addHandler(logging.StreamHandler())
        log.setLevel(logging.INFO)

        broker_url = "test.kafka:9000"
        registry_url = "https://registry.test.kafka/"
        partitions = 2
        replication_factor = 3
        wait_for_ack = 1

        self.kafka_info = salkafka.KafkaInfo(
            broker_url=broker_url,
            registry_url=registry_url,
            partitions=partitions,
            replication_factor=replication_factor,
            wait_for_ack=wait_for_ack,
            log=log,
        )
        self.topic_producer = salkafka.TopicProducer(
            kafka_info=self.kafka_info, topic=read_topic, log=log
        )
        await asyncio.gather(
            self.topic_producer.start_task,
            self.kafka_info.start_task,
            self.read_salinfo.start(),
            self.csc.start_task,
        )

    async def test_basics(self):
        await self.make_producer(topic_name="arrays", sal_prefix="logevent_")
        for isample in range(3):
            evt_array_data = self.csc.make_random_evt_arrays()
            self.csc.evt_arrays.put(evt_array_data)
            for iread in range(10):
                if self.topic_producer.n_data_sent > isample:
                    break
                await asyncio.sleep(0.01)
            else:
                self.fail("Data not seen in time")
            self.assertEqual(self.topic_producer.n_data_sent, isample + 1)

    async def test_ackcmd(self):
        """ackcmd topics are special, so make sure they work.

        This exercises DM-25707
        """
        await self.make_producer(topic_name="ackcmd", sal_prefix="")
        for isample in range(3):
            self.csc.salinfo._ackcmd_writer.set(private_seqNum=isample)
            self.csc.salinfo._ackcmd_writer.put()
            for iread in range(10):
                if self.topic_producer.n_data_sent > isample:
                    break
                await asyncio.sleep(0.01)
            else:
                self.fail("Data not seen in time")
            self.assertEqual(self.topic_producer.n_data_sent, isample + 1)


if __name__ == "__main__":
    unittest.main()
