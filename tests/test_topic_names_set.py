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

import pathlib
import unittest

import jsonschema
import pytest

from lsst.ts import idl
from lsst.ts import salobj
from lsst.ts import salkafka


class TopicNamesSetTestCase(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.component = "Test"
        cls.data_dir = pathlib.Path(__file__).parent / "data" / "topic_names_sets"

        metadata = salobj.parse_idl(
            cls.component, idl.get_idl_dir() / "sal_revCoded_Test.idl"
        )
        command_names = []
        event_names = []
        telemetry_names = []
        for topic_metadata in metadata.topic_info.values():
            sal_topic_name = topic_metadata.sal_name
            if sal_topic_name.startswith("command_"):
                command_names.append(sal_topic_name[8:])
            elif sal_topic_name.startswith("logevent_"):
                event_names.append(sal_topic_name[9:])
            elif sal_topic_name != "ackcmd":
                telemetry_names.append(sal_topic_name)

        cls.command_names = tuple(command_names)
        cls.event_names = tuple(event_names)
        cls.telemetry_names = tuple(telemetry_names)
        cls.all_names = dict(
            commands=cls.command_names,
            events=cls.event_names,
            telemetry=cls.telemetry_names,
        )
        cls.categories = ("commands", "events", "telemetry")

    def check_no_partitions(self, topic_names_set):
        """Check a TopicNamesSet constructed with no TopicNames
        specified.
        """
        assert topic_names_set.component == self.component
        assert topic_names_set.queue_len == salobj.topics.DEFAULT_QUEUE_LEN
        assert len(topic_names_set.topic_names_list) == 1
        topic_names = topic_names_set.topic_names_list[0]
        assert topic_names.add_ackcmd
        assert topic_names.commands == sorted(self.command_names)
        assert topic_names.events == sorted(self.event_names)
        assert topic_names.telemetry == sorted(self.telemetry_names)

    def test_no_partitions(self):
        """Test that specifying an empty list of topic_names_list
        results in a single TopicNames instance with all topics.
        """
        topic_names_set = salkafka.TopicNamesSet(component="Test", topic_names_list=[])
        self.check_no_partitions(topic_names_set)

    def test_two_partitions(self):
        kwargs0 = {}
        kwargs1 = {}
        for category in self.categories:
            kwargs0[category] = self.all_names[category][0:2]
            kwargs1[category] = self.all_names[category][2:4]
        kwargs1["add_ackcmd"] = True
        partition0 = salkafka.TopicNames(**kwargs0)
        partition1 = salkafka.TopicNames(**kwargs1)

        # Check the values in the topic names list
        assert not partition0.add_ackcmd
        assert partition1.add_ackcmd
        for category in self.categories:
            assert getattr(partition0, category) == sorted(kwargs0[category])
            assert getattr(partition1, category) == sorted(kwargs1[category])

        # Create and check the topic names
        queue_len = 2500
        topic_names_set = salkafka.TopicNamesSet(
            component="Test",
            queue_len=queue_len,
            topic_names_list=[partition0, partition1],
        )
        assert topic_names_set.queue_len == queue_len
        assert len(topic_names_set.topic_names_list) == 3

        # Check that the TopicNames item match the input
        assert vars(partition0) == vars(topic_names_set.topic_names_list[0])
        assert vars(partition1) == vars(topic_names_set.topic_names_list[1])

        # Check the values in the extra TopicNames entry
        extra_partition = topic_names_set.topic_names_list[2]
        for category in self.categories:
            remaining_names = self.all_names[category][4:]
            assert getattr(extra_partition, category) == sorted(remaining_names)

    def test_bad_topic_names(self):
        for category in self.categories:
            bad_kwargs = {category: "no_such_topic"}
            bad_partition = salkafka.TopicNames(**bad_kwargs)
            with pytest.raises(ValueError):
                salkafka.TopicNamesSet(
                    component="Test", topic_names_list=[bad_partition]
                )

    def test_duplicate_topic_names(self):
        for category in self.categories:
            all_names = self.all_names[category]
            kwargs0 = {category: all_names[0:2]}
            kwargs1 = {category: all_names[1:3]}
            partition0 = salkafka.TopicNames(**kwargs0)
            partition1 = salkafka.TopicNames(**kwargs1)
            with pytest.raises(ValueError):
                salkafka.TopicNamesSet(
                    component="Test", topic_names_list=[partition0, partition1]
                )
        topic_names = salkafka.TopicNames(add_ackcmd=True)
        with pytest.raises(ValueError):
            salkafka.TopicNamesSet(
                component="Test", topic_names_list=[topic_names, topic_names]
            )

    def test_from_file_good(self):
        topic_names_set = salkafka.TopicNamesSet.from_file(
            self.data_dir / "good_no_partitions.yaml"
        )
        self.check_no_partitions(topic_names_set)

        topic_names_set = salkafka.TopicNamesSet.from_file(
            self.data_dir / "good_two_partitions.yaml"
        )
        assert topic_names_set.component == "Test"
        assert topic_names_set.queue_len == 2500
        assert len(topic_names_set.topic_names_list) == 3
        partition0 = topic_names_set.topic_names_list[0]
        assert partition0.add_ackcmd
        assert partition0.commands == ["setArrays", "setScalars"]
        assert partition0.events == []
        assert partition0.telemetry == []
        partition1 = topic_names_set.topic_names_list[1]
        assert not partition1.add_ackcmd
        assert partition1.commands == []
        assert partition1.events == ["arrays"]
        assert partition1.telemetry == ["scalars"]

    def test_from_file_bad(self):
        for filepath in self.data_dir.glob("bad_*.yaml"):
            if "duplicate" in filepath.name:
                with pytest.raises(ValueError):
                    salkafka.TopicNamesSet.from_file(filepath)
            else:
                with pytest.raises(jsonschema.ValidationError):
                    salkafka.TopicNamesSet.from_file(filepath)
