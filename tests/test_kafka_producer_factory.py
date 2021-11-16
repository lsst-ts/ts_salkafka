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

import logging
import unittest

import pytest

from lsst.ts import salkafka


class KafkaProducerFactoryTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Make a valid KafkaConfiguration argument dict with arbitrary data
        self.config_kwargs = dict(
            broker_url="test.kafka:9000",
            registry_url="https://registry.test.kafka/",
            partitions=2,
            replication_factor=3,
            wait_for_ack="all",
        )

    def run(self, result=None):
        """Override `run` to insert mocks for every test.

        https://stackoverflow.com/a/11180583
        """
        with salkafka.mocks.insert_all_mocks():
            super().run(result)

    async def test_kafka_configuration(self):
        kafka_config = salkafka.KafkaConfiguration(**self.config_kwargs)
        for name, value in self.config_kwargs.items():
            assert getattr(kafka_config, name) == value

        for name, bad_value in (
            ("wait_for_ack", 2),
            ("wait_for_ack", -1),
            ("wait_for_ack", None),
            ("wait_for_ack", "some"),
        ):
            bad_config_kwargs = self.config_kwargs.copy()
            bad_config_kwargs[name] = bad_value
            with pytest.raises(ValueError):
                salkafka.KafkaConfiguration(**bad_config_kwargs)

    async def test_make_kafka_topics(self):
        config = salkafka.KafkaConfiguration(**self.config_kwargs)
        log = logging.getLogger()

        async with salkafka.KafkaProducerFactory(
            config=config, log=log
        ) as kafka_factory:
            existing_topic_names = ["old_topic", "another_old_topic"]
            kafka_factory.broker_client.set_existing_topic_names(existing_topic_names)
            new_topic_names = ["new_topic", "another_new_topic"]
            all_topic_names = existing_topic_names + new_topic_names
            created_topic_names = kafka_factory.make_kafka_topics(all_topic_names)
            assert set(new_topic_names) == set(created_topic_names)

    async def test_make_producer(self):
        topic_name = "lsst.sal.Test.foo"
        config = salkafka.KafkaConfiguration(**self.config_kwargs)
        log = logging.getLogger()

        async with salkafka.KafkaProducerFactory(
            config=config,
            log=log,
        ) as kafka_factory:
            avro_schema = {
                "name": topic_name,
                "type": "record",
                "fields": [{"name": "TestID", "type": "long"}],
            }
            producer = await kafka_factory.make_producer(avro_schema=avro_schema)
            assert producer.bootstrap_servers == config.broker_url
            assert producer.sent_data == []
            expected_name_value_list = []
            for i in range(10):
                value = {"TestID": i}
                expected_name_value_list.append((topic_name, value))
                await producer.send_and_wait(topic_name, value=value)
            sent_name_value_list = [item[0:2] for item in producer.sent_data]
            assert sent_name_value_list == expected_name_value_list
            for item in producer.sent_data:
                assert isinstance(item[2], bytes)

            invalid_value = {"unexpected_key": "some_value"}
            with pytest.raises(Exception):
                # the actual exception is not documented
                await producer.send_and_wait(topic_name, value=invalid_value)
