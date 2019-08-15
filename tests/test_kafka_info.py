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

from lsst.ts import salkafka


class KafkaInfoTestCase(unittest.TestCase):
    def run(self, result=None):
        """Override `run` to insert mocks for every test.

        https://stackoverflow.com/a/11180583
        """
        with salkafka.mocks.insert_all_mocks():
            super().run(result)

    def test_constructor(self):
        broker_url = "test.kafka:9000"
        registry_url = "https://registry.test.kafka/"
        partitions = 2
        replication_factor = 3
        # wait_for_ack 0 and 1 are unchanged but 2 becomes "all"
        wait_for_ack = 2
        desired_wait_for_ack = "all"
        log = logging.getLogger()

        async def doit():
            async with salkafka.KafkaInfo(broker_url=broker_url,
                                          registry_url=registry_url,
                                          partitions=partitions,
                                          replication_factor=replication_factor,
                                          wait_for_ack=wait_for_ack,
                                          log=log) as kafka_info:
                self.assertEqual(kafka_info.broker_url, broker_url)
                self.assertEqual(kafka_info.registry_url, registry_url)
                self.assertEqual(kafka_info.replication_factor, replication_factor)
                self.assertEqual(kafka_info.broker_url, broker_url)
                self.assertEqual(kafka_info.wait_for_ack, desired_wait_for_ack)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_make_kafka_topics(self):
        broker_url = "test.kafka:9000"
        registry_url = "https://registry.test.kafka/"
        partitions = 1
        replication_factor = 3
        wait_for_ack = 1
        log = logging.getLogger()

        async def doit():
            async with salkafka.KafkaInfo(broker_url=broker_url,
                                          registry_url=registry_url,
                                          partitions=partitions,
                                          replication_factor=replication_factor,
                                          wait_for_ack=wait_for_ack,
                                          log=log) as kafka_info:
                existing_topic_names = ["old_topic", "another_old_topic"]
                kafka_info.broker_client.set_existing_topic_names(existing_topic_names)
                new_topic_names = ["new_topic", "another_new_topic"]
                all_topic_names = existing_topic_names + new_topic_names
                created_topic_names = kafka_info.make_kafka_topics(all_topic_names)
                self.assertEqual(set(new_topic_names), set(created_topic_names))

        asyncio.get_event_loop().run_until_complete(doit())

    def test_make_producer(self):
        broker_url = "test.kafka:9000"
        registry_url = "https://registry.test.kafka/"
        topic_name = "lsst.sal.Test.foo"
        partitions = 1
        replication_factor = 3
        wait_for_ack = 1
        log = logging.getLogger()

        async def doit():
            async with salkafka.KafkaInfo(broker_url=broker_url,
                                          registry_url=registry_url,
                                          partitions=partitions,
                                          replication_factor=replication_factor,
                                          wait_for_ack=wait_for_ack,
                                          log=log) as kafka_info:
                avro_schema = {"name": topic_name,
                               "type": "record",
                               "fields": [{"name": "TestID", "type": "long"}]}
                producer = await kafka_info.make_producer(avro_schema=avro_schema)
                self.assertEqual(producer.bootstrap_servers, broker_url)
                self.assertEqual(producer.sent_data, [])
                expected_name_value_list = []
                for i in range(10):
                    value = {"TestID": i}
                    expected_name_value_list.append((topic_name, value))
                    await producer.send_and_wait(topic_name, value=value)
                sent_name_value_list = [item[0:2] for item in producer.sent_data]
                self.assertEqual(sent_name_value_list, expected_name_value_list)
                for item in producer.sent_data:
                    self.assertIsInstance(item[2], bytes)

                invalid_value = {"unexpected_key": "some_value"}
                with self.assertRaises(Exception):
                    # the actual exception is not documented
                    await producer.send_and_wait(topic_name, value=invalid_value)

        asyncio.get_event_loop().run_until_complete(doit())


if __name__ == "__main__":
    unittest.main()
