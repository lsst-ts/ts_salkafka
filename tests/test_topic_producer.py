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

import numpy as np

from lsst.ts import salobj
from lsst.ts import salkafka


class Harness:
    def __init__(self, topic_attr_name):
        salobj.set_random_lsst_dds_domain()

        # Use a non-zero index for the producer to test that
        # the topic producer can see it.
        self.csc = salobj.TestCsc(index=5)

        # Always use an index of 0 for the TopicProducer's read topic
        # (which we get from the remote).
        self.remote = salobj.Remote(domain=self.csc.domain, name="Test", index=0)
        read_topic = getattr(self.remote, topic_attr_name)

        log = logging.getLogger()
        log.addHandler(logging.StreamHandler())
        log.setLevel(logging.INFO)

        broker_url = "test.kafka:9000"
        registry_url = "https://registry.test.kafka/"
        partitions = 2
        replication_factor = 3
        wait_for_ack = 1

        self.kafka_info = salkafka.KafkaInfo(broker_url=broker_url,
                                             registry_url=registry_url,
                                             partitions=partitions,
                                             replication_factor=replication_factor,
                                             wait_for_ack=wait_for_ack,
                                             log=log)
        self.topic_producer = salkafka.TopicProducer(kafka_info=self.kafka_info,
                                                     topic=read_topic,
                                                     log=log)

    async def __aenter__(self):
        await asyncio.gather(self.topic_producer.start_task,
                             self.kafka_info.start_task,
                             self.remote.start_task,
                             self.csc.start_task)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await asyncio.gather(self.topic_producer.close(),
                             self.kafka_info.close(),
                             self.remote.close(),
                             self.csc.close())


class TopicProducerTestCase(unittest.TestCase):
    def run(self, result=None):
        """Override `run` to insert mocks for every test.

        https://stackoverflow.com/a/11180583
        """
        with salkafka.mocks.insert_all_mocks():
            super().run(result)

    def setUp(self):
        np.random.seed(47)

    def test_basics(self):
        async def doit():
            async with Harness(topic_attr_name="evt_arrays") as harness:
                for isample in range(3):
                    evt_array_data = harness.csc.make_random_evt_arrays()
                    harness.csc.evt_arrays.put(evt_array_data)
                    for iread in range(10):
                        if len(harness.topic_producer.kafka_producer.sent_data) > isample:
                            break
                        await asyncio.sleep(0.01)
                    else:
                        self.fail("Data not seen in time")
                    self.assertEqual(len(harness.topic_producer.kafka_producer.sent_data), isample+1)
                    kafka_topic_name, sent_value, serialized_value = \
                        harness.topic_producer.kafka_producer.sent_data[-1]
                    self.assertEqual(kafka_topic_name, "lsst.sal.Test.logevent_arrays")
                    self.assertIsInstance(serialized_value, bytes)
                    for key, value in evt_array_data.get_vars().items():
                        if key == "private_rcvStamp":
                            # not set in evt_array_data but set in received
                            # sample and thus in ``sent_value``
                            continue
                        if isinstance(value, np.ndarray):
                            np.testing.assert_array_equal(sent_value[key], value)
                        else:
                            self.assertEqual(sent_value[key], value)

        asyncio.get_event_loop().run_until_complete(doit())


if __name__ == "__main__":
    unittest.main()
