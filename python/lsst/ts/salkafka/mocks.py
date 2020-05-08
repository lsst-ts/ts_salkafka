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

__all__ = [
    "MockKafkitRegistryApi",
    "MockAIOKafkaProducer",
    "MockConfluentAdminClient",
    "insert_all_mocks",
]

import contextlib
import types

from . import kafka_info


class MockKafkitRegistryApi:
    """Mock `kafkit.registry.aiohttp.RegistryApi`.
    """

    def __init__(self, session, url):
        self.session = session
        self.url = url

    async def register_schema(self, schema, subject):
        self.schema = schema
        self.subject = subject
        return 100  # an arbitrary ID


class MockAIOKafkaProducer:
    """Mock `aiokafka.AIOKafkaProducer`.
    """

    def __init__(self, *, loop, bootstrap_servers, acks, value_serializer, **kwargs):
        self.loop = loop
        self.bootstrap_servers = bootstrap_servers
        self.acks = acks
        self.value_serializer = value_serializer
        self.kwargs = kwargs
        self.sent_data = []
        """A list of (name, value, serialized_value).

        Every call to ``send_and_wait`` appends one item.
        """

    async def start(self):
        pass

    async def stop(self):
        pass

    async def send_and_wait(self, name, value):
        serialzed_value = self.value_serializer(value)
        self.sent_data.append((name, value, serialzed_value))


class _MockConfluentFuture:
    def result(self):
        pass


class MockConfluentAdminClient:
    """Mock `confluent_kafka.admin.AdminClient`.

    Created with a small set of existing topic names;
    you can replace them with `set_existing_topic_names`.
    """

    def __init__(self, arg):
        self.arg = arg
        existing_topic_names = [
            "lsst.sal.Test.command_start",
            "lsst.sal.Test.logevent_summaryState",
        ]
        self.set_existing_topic_names(existing_topic_names)

    def set_existing_topic_names(self, existing_topic_names):
        """Specify topics that are already on the server.

        This replaces ``self.existing_topic_names``.
        This is not a standard method of `confluent_kafka.admin.AdminClient`.
        """
        self.existing_topic_names = existing_topic_names
        datadict = dict((name, None) for name in self.existing_topic_names)
        self.metadata = types.SimpleNamespace(topics=datadict)

    def list_topics(self, timeout):
        return self.metadata

    def create_topics(self, new_topic_metadata):
        class CreateTopicsReturn:
            def __init__(self, new_topic_metadata):
                self.new_topic_metadata = new_topic_metadata

            def items(self):
                return [
                    (nt.topic, _MockConfluentFuture()) for nt in self.new_topic_metadata
                ]

        return CreateTopicsReturn(new_topic_metadata)


# dict of class name: mock class
_MOCK_CLASSES = dict(
    RegistryApi=MockKafkitRegistryApi,
    AIOKafkaProducer=MockAIOKafkaProducer,
    AdminClient=MockConfluentAdminClient,
)

# dict of class name: real class
_REAL_CLASSES = dict((name, getattr(kafka_info, name)) for name in _MOCK_CLASSES)


@contextlib.contextmanager
def insert_all_mocks(*args, **kwds):
    """Context manager to replace all real classes with mocks.
    """
    try:
        for name, MockClass in _MOCK_CLASSES.items():
            setattr(kafka_info, name, MockClass)
        yield
    finally:
        for name, RealClass in _REAL_CLASSES.items():
            setattr(kafka_info, name, RealClass)
