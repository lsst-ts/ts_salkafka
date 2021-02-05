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

    async def setUp(self):
        salobj.set_random_lsst_dds_domain()
        # Use a non-zero index for the producer to test that
        # the topic producer can see it.
        self.csc = salobj.TestCsc(index=5)

        # Always use an index of 0 for the TopicProducer's read topic
        # (which we get from the remote).
        self.remote = salobj.Remote(domain=self.csc.domain, name="Test", index=0)

        log = logging.getLogger()
        log.addHandler(logging.StreamHandler())
        log.setLevel(logging.INFO)

        broker_url = "test.kafka:9000"
        registry_url = "https://registry.test.kafka/"
        partitions = 2
        replication_factor = 3
        wait_for_ack = 1

        kafka_config = salkafka.KafkaConfiguration(
            broker_url=broker_url,
            registry_url=registry_url,
            partitions=partitions,
            replication_factor=replication_factor,
            wait_for_ack=wait_for_ack,
        )
        self.kafka_factory = salkafka.KafkaProducerFactory(
            config=kafka_config, log=log,
        )
        self.component_producer = salkafka.ComponentProducer(
            domain=self.csc.domain, name="Test", kafka_factory=self.kafka_factory
        )

        await asyncio.gather(
            self.component_producer.start_task,
            self.kafka_factory.start_task,
            self.remote.start_task,
            self.csc.start_task,
        )

    async def tearDown(self):
        await asyncio.gather(
            self.component_producer.close(),
            self.kafka_factory.close(),
            self.remote.close(),
            self.csc.close(),
        )

    async def test_basics(self):
        attr_names = ["ack_ackcmd"]
        attr_names += ["cmd_" + name for name in self.csc.salinfo.command_names]
        attr_names += ["evt_" + name for name in self.csc.salinfo.event_names]
        attr_names += ["tel_" + name for name in self.csc.salinfo.telemetry_names]
        self.assertEqual(
            set(attr_names), set(self.component_producer.topic_producers.keys())
        )

        producer = self.component_producer.topic_producers["evt_arrays"]
        for isample in range(3):
            evt_array_data = self.csc.make_random_evt_arrays()
            self.csc.evt_arrays.put(evt_array_data)
            for iread in range(10):
                if len(producer.kafka_producer.sent_data) > isample:
                    break
                await asyncio.sleep(0.01)
            else:
                self.fail("Data not seen in time")
            self.assertEqual(len(producer.kafka_producer.sent_data), isample + 1)
            (
                kafka_topic_name,
                sent_value,
                serialized_value,
            ) = producer.kafka_producer.sent_data[-1]
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

        producer = self.component_producer.topic_producers["evt_scalars"]
        for isample in range(3):
            evt_scalar_data = self.csc.make_random_evt_scalars()
            self.csc.evt_scalars.put(evt_scalar_data)
            for iread in range(10):
                if len(producer.kafka_producer.sent_data) > isample:
                    break
                await asyncio.sleep(0.01)
            else:
                self.fail("Data not seen in time")
            self.assertEqual(len(producer.kafka_producer.sent_data), isample + 1)
            (
                kafka_topic_name,
                sent_value,
                serialized_value,
            ) = producer.kafka_producer.sent_data[-1]
            self.assertEqual(kafka_topic_name, "lsst.sal.Test.logevent_scalars")
            self.assertIsInstance(serialized_value, bytes)
            for key, value in evt_scalar_data.get_vars().items():
                if key == "private_rcvStamp":
                    # not set in evt_scalar_data but set in received
                    # sample and thus in ``sent_value``
                    continue
                self.assertEqual(sent_value[key], value)


if __name__ == "__main__":
    unittest.main()
