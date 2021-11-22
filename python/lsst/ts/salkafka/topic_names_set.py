from __future__ import annotations

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

__all__ = ["TopicNames", "TopicNamesSet"]

import dataclasses
import collections.abc
import yaml

import jsonschema

from lsst.ts import idl
from lsst.ts import salobj

TOPIC_CATEGORIES = ("commands", "events", "telemetry")


@dataclasses.dataclass
class TopicNames:
    """A collection of topic names.

    Topic names must *omit* the ``command_`` and ``logevent_`` prefix.
    For example specify "start" instead of "command_start",
    and "summaryState" instead of "logevent_summaryState".

    Parameters
    ----------
    partitions : `int`
        The desired number of Kafka partitions for these topics.
    add_ackcmd : `bool`, optional
        Add ``ackcmd`` topic to the producer?
    commands : `list` of `str`, optional
        Commands to add to the producer, with no prefix, e.g. "enable".
        Converted to a sorted list.
    events : `list` of `str`, optional
        Events to add to the producer, with no prefix, e.g. "summaryState".
        Converted to a sorted list.
    telemetry : `list` of `str`, optional
        Telemtry topics to add to the producer.
        Converted to a sorted list.
    """

    partitions: int
    add_ackcmd: bool = False
    commands: collections.abc.Sequence[str] = ()
    events: collections.abc.Sequence[str] = ()
    telemetry: collections.abc.Sequence[str] = ()

    def __post_init__(self):
        for category in TOPIC_CATEGORIES:
            sorted_list = sorted(getattr(self, category))
            setattr(self, category, sorted_list)


class TopicNamesSet:
    """A complete collection of `TopicNames` for a given SAL component.

    This is designed to support running producers for subsets of topics
    in separate subprocesses. However, this class only contains information;
    it has no support for subprocesses or producing messages.

    All topic names omit the ``command_`` and ``logevent_`` prefix.
    For example specify "start" instead of "command_start",
    and "summaryState" instead of "logevent_summaryState".

    The ``topic_names_list`` attribute includes all topics.

    Parameters
    ----------
    component : `str`
        Name of a SAL component for which to handle a subset of topics.
    topic_names_list : `List` [`TopicNames`]
        List of `TopicNames` entries. Need not be complete, but if any topics
        are missing, the ``topic_names_list`` attribute has one additional item
        that specifies all remaining topics.
    default_partitions : `int`, optional
        The default number of Kafka partitions for each topic.
    queue_len : `int`, optional
        Length of the read queue.

    Raises
    ------
    ValueError
        If an ``topic_names_list`` entry contains any invalid names
        or duplicates with prior information.
    """

    def __init__(
        self,
        *,
        component,
        topic_names_list,
        default_partitions=1,
        queue_len=salobj.topics.DEFAULT_QUEUE_LEN,
    ):
        self.component = component
        self.default_partitions = default_partitions
        self.topic_names_list = list(topic_names_list)
        self.queue_len = int(queue_len)

        # Construct a dict of topic category: all topic names, by reading the
        # IDL file. Topic names *omit* the command_ and logevent_ prefix.
        all_names_dict = {category: set() for category in TOPIC_CATEGORIES}
        idl_metadata = salobj.parse_idl(
            component, idl.get_idl_dir() / f"sal_revCoded_{component}.idl"
        )
        for topic in idl_metadata.topic_info:
            if topic.startswith("command_"):
                all_names_dict["commands"].add(topic[8:])
            elif topic.startswith("logevent_"):
                all_names_dict["events"].add(topic[9:])
            elif topic == "ackcmd":
                pass
            else:
                all_names_dict["telemetry"].add(topic)

        # Examine the topic_names_list, recording which topics have been seen
        # and looking for invalid and duplicate names.
        ackcmd_added = False
        seen_names_dict = {category: set() for category in TOPIC_CATEGORIES}
        for i, topic_names in enumerate(self.topic_names_list):
            if topic_names.add_ackcmd:
                if ackcmd_added:
                    raise ValueError(
                        f"topic_names_list[{i}] invalid: ackcmd already added"
                    )
                ackcmd_added = True

            for category in TOPIC_CATEGORIES:
                all_names = all_names_dict[category]
                seen_names = seen_names_dict[category]
                specified_names = set(getattr(topic_names, category, []))

                # Check that specified names are valid
                invalid_names = specified_names - all_names
                if invalid_names:
                    raise ValueError(
                        f"topic_names_list[{i}].{category} "
                        f"has invalid names: {sorted(invalid_names)}"
                    )

                # Check for duplicates
                duplicate_names = seen_names & specified_names
                if duplicate_names:
                    raise ValueError(
                        f"topic_names_list[{i}].{category} "
                        f"has names already specified: {sorted(duplicate_names)}"
                    )

                seen_names |= specified_names

        # Append a TopicNames to handle remaining topics, if needed.
        remaining_names = {
            category: all_names_dict[category] - seen_names_dict[category]
            for category in TOPIC_CATEGORIES
        }
        if not ackcmd_added or any(
            len(remaining_names[category]) > 0 for category in TOPIC_CATEGORIES
        ):
            self.topic_names_list.append(
                TopicNames(
                    partitions=self.default_partitions,
                    add_ackcmd=not ackcmd_added,
                    **remaining_names,
                )
            )

    @classmethod
    def from_file(cls, filename, default_partitions=1):
        """Create from a file path.

        Parameters
        ----------
        filename : `str`
            Path of yaml file to load.
            The schema must match that from `schema`
        default_partitions : `int`, optional
            The default number of Kafka partitions for each topic.

        Returns
        -------
        topic_split : `SplitTopic`
            Data for the split topics.
        """
        # First step is to validate the input schema.
        validator = jsonschema.Draft7Validator(cls.schema())

        with open(filename) as fp:
            components_info = yaml.safe_load(fp)

        validator.validate(components_info)

        topic_names_list = []
        component = components_info["component"]
        for topic_set in components_info.get("topic_sets", []):
            topic_set.setdefault("partitions", default_partitions)
            topic_names_list.append(TopicNames(**topic_set))
        queue_len = components_info.get("queue_len", salobj.topics.DEFAULT_QUEUE_LEN)
        return cls(
            component=component,
            topic_names_list=topic_names_list,
            default_partitions=default_partitions,
            queue_len=queue_len,
        )

    @staticmethod
    def schema():
        return yaml.safe_load(
            f"""
        $schema: http://json-schema.org/draft-07/schema#
        $id: https://github.com/lsst-ts/ts_salkafka/salkafka.yaml
        description: Configuration for the salkafka producer.
        type: object
        additionalProperties: false
        properties:
          component:
            description: Name of SAL component, e.g. "Test".
            type: string
          queue_len:
            description: Length of the python queue on the topic readers.
            type: integer
            exclusiveMinimum: {salobj.topics.DEFAULT_QUEUE_LEN}
          topic_sets:
            description: >-
                List of sets of topic names.
                Each item specifies the topics for one producer subprocess.
                An additional producer subprocess is added to handle all remaining
                topics, if there are any that you did not specify.
                It is an error to specify the same topic more than once
                or to specify a topic that does not exist.
            type: array
            items:
              type: object
              additionalProperties: false
              properties:
                add_ackcmd:
                  description: Add command acknowledgements?
                  type: boolean
                commands:
                  description: >-
                    List of command names (without the "command_" prefix) to add to producer.
                    If omitted or [] then handle no commands.
                  type: array
                  items:
                    type: string
                events:
                  description: >-
                    List of event names (without the "logevent_" prefix) to add to producer,
                    If omitted or [] then handle no events.
                  type: array
                  items:
                    type: string
                telemetry:
                  description: >-
                    List of telemetry topic names to add to producer,
                    If omitted or [] then handle no telemetry.
                  type: array
                  items:
                    type: string
                partitions:
                    description: >-
                        The number of Kafka partitions.
                        If omitted, use the value specified by the
                        --partitions command-line argument.
                    type: integer
                    minimum: 1
        required:
          - component
          - topic_sets
        """
        )
