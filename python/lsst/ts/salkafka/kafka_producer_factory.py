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

__all__ = ["KafkaConfiguration", "KafkaProducerFactory"]

import asyncio
import dataclasses
import typing

import aiohttp

# use `from x import y` to support replacing these classes with mocks
from aiokafka import AIOKafkaProducer
from confluent_kafka.admin import AdminClient, NewTopic
from kafkit.registry.aiohttp import RegistryApi
from kafkit.registry import Serializer

SECURITY_PROTOCOL = "SASL_PLAINTEXT"
SASL_MECHANISM = "SCRAM-SHA-512"


@dataclasses.dataclass
class KafkaConfiguration:
    """Kakfa producer configuration.

    Parameters
    ----------
    broker_url : `str`
        Kafka broker URL, without the transport.
        For example: ``my.kafka:9000``
    registry_url : `str`
        Schema Registry URL, including the transport.
        For example: ``https://registry.my.kafka/``
    partitions : `int`
        Number of partitions for each Kafka topic.
    replication_factor : `int`
        Number of replicas for each Kafka partition.
    wait_for_ack : `int` or `str`
        The only allowd values are:

        * 0: do not wait (unsafe)
        * 1: wait for first kafka broker to respond (recommended)
        * "all": wait for all kafka brokers to respond
    sasl_plain_username : `str`
        username for SASL authentication.
        Default: None
    sasl_plain_password : `str`
        password for SASL authentication.
        Default: None
    """

    broker_url: str
    registry_url: str
    partitions: int
    replication_factor: int
    wait_for_ack: typing.Union[int, str]
    sasl_plain_username: str = None
    sasl_plain_password: str = None

    def __post_init__(self):
        if self.wait_for_ack not in (0, 1, "all"):
            raise ValueError(
                f"wait_for_ack={self.wait_for_ack!r} must be one of 0, 1, 'all'"
            )


class KafkaProducerFactory:
    """Factory for making Kafka producers.

    Parameters
    ----------
    config : `KafkaConfiguration`
        Kafka arguments.
    log : `logging.Logger`
        Logger.
    """

    def __init__(self, config, log):
        self.config = config
        self.log = log

        self.http_session = None  # created by `start`
        self.schema_registry = None
        self.log.info("Making Kafka client session")
        if self.config.sasl_plain_username and self.config.sasl_plain_password:
            self.broker_client = AdminClient(
                {
                    "bootstrap.servers": self.config.broker_url,
                    "security.protocol": SECURITY_PROTOCOL,
                    "sasl.mechanisms": SASL_MECHANISM,
                    "sasl.username": self.config.sasl_plain_username,
                    "sasl.password": self.config.sasl_plain_password,
                }
            )
        else:
            self.broker_client = AdminClient(
                {"bootstrap.servers": self.config.broker_url}
            )
        self.start_task = asyncio.ensure_future(self.start())

    async def start(self):
        """Start the Kafka clients."""
        self.log.info("Making avro schema registry.")
        connector = aiohttp.TCPConnector(limit_per_host=20)
        self.http_session = aiohttp.ClientSession(connector=connector)
        self.schema_registry = RegistryApi(
            session=self.http_session, url=self.config.registry_url
        )

    async def close(self):
        """Close the Kafka clients."""
        if self.http_session is not None:
            await self.http_session.close()

    def make_kafka_topics(self, topic_names):
        """Initialize Kafka topics that do not already exist.

        Parameters
        ----------
        topic_names : `list`[ `str` ]
            List of Kafka topic names.

        Returns
        -------
        new_topic_names : `list` [`str`]
            List of newly created Kafka topic names.
        """
        metadata = self.broker_client.list_topics(timeout=10)
        existing_topic_names = set(metadata.topics.keys())
        new_topic_names = sorted(set(topic_names) - existing_topic_names)
        if len(new_topic_names) == 0:
            return []
        new_topic_metadata = [
            NewTopic(
                topic_name,
                num_partitions=self.config.partitions,
                replication_factor=self.config.replication_factor,
            )
            for topic_name in new_topic_names
        ]
        fs = self.broker_client.create_topics(new_topic_metadata)
        for topic_name, future in fs.items():
            try:
                future.result()  # The result itself is None
                self.log.debug(f"Created topic {topic_name}")
            except Exception:
                self.log.exception(f"Failed to create topic {topic_name}")
                raise
        return new_topic_names

    async def make_producer(self, avro_schema):
        """Make and start a Kafka producer for a topic.

        Parameters
        ----------
        avro_schema : `dict`
            Avro schema for the topic.

        Returns
        -------
        producer : `aiokafka.AIOKafkaProducer`
            Kafka message producer.
        """
        serializer = await Serializer.register(
            registry=self.schema_registry,
            schema=avro_schema,
            subject=f"{avro_schema['name']}-value",
        )
        if self.config.sasl_plain_username and self.config.sasl_plain_password:
            producer = AIOKafkaProducer(
                loop=asyncio.get_running_loop(),
                bootstrap_servers=self.config.broker_url,
                acks=self.config.wait_for_ack,
                value_serializer=serializer,
                security_protocol=SECURITY_PROTOCOL,
                sasl_mechanism=SASL_MECHANISM,
                sasl_plain_username=self.config.sasl_plain_username,
                sasl_plain_password=self.config.sasl_plain_password,
            )
        else:
            producer = AIOKafkaProducer(
                loop=asyncio.get_running_loop(),
                bootstrap_servers=self.config.broker_url,
                acks=self.config.wait_for_ack,
                value_serializer=serializer,
            )
        await producer.start()
        return producer

    async def __aenter__(self):
        await self.start_task
        return self

    async def __aexit__(self, type, value, traceback):
        await self.close()
