#!/usr/bin/env python
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

__all__ = ["ComponentProducerSet"]

import argparse
import asyncio
import functools
import jsonschema
import logging
import os
import pathlib
import signal
import yaml

import concurrent.futures

from lsst.ts import salobj
from lsst.ts import salkafka


class ComponentProducerSet:
    r"""A collection of one or more `ComponentProducer`\ s
    created from a command-line script.

    Parameters
    ----------
    args : `argparse.Namespace`
        Parsed command-line arguments using the argument parser
        from `make_argument_parser`.
    """

    def __init__(
        self,
        args,
        component=None,
        add_ack=None,
        commands=None,
        events=None,
        telemetry=None,
    ):

        self.log = logging.getLogger()
        self.log.addHandler(logging.StreamHandler())
        self.log.setLevel(args.loglevel)

        semaphore_filename = "SALKAFKA_PRODUCER_RUNNING"
        suffix = os.environ.get("SALKAFKA_SEMAPHORE_SUFFIX")
        if suffix is not None:
            semaphore_filename += f"_{suffix.upper()}"

        self.semaphore_file = pathlib.Path("/tmp", semaphore_filename)
        if self.semaphore_file.exists():
            self.semaphore_file.unlink()

        self.producers = []

        # A task that ends when the service is interrupted
        self.wait_forever_task = asyncio.Future()

        self.done_task = asyncio.get_event_loop().create_task(
            self.run(
                args,
                component=component,
                add_ack=add_ack,
                commands=commands,
                events=events,
                telemetry=telemetry,
            )
        )

    async def run(
        self,
        args,
        component=None,
        add_ack=False,
        commands=None,
        events=None,
        telemetry=None,
    ):
        """Create and run the component producers.
        """
        async with salobj.Domain() as domain, salkafka.KafkaInfo(
            broker_url=args.broker_url,
            registry_url=args.registry_url,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
            wait_for_ack=args.wait_for_ack,
            log=self.log,
        ) as kafka_info:
            self.domain = domain
            self.kafka_info = kafka_info

            self.producers = []
            if component is None:
                self.log.info("Creating producers")
                for name in args.components:
                    self.producers.append(
                        salkafka.ComponentProducer(
                            domain=domain, name=name, kafka_info=kafka_info
                        )
                    )
            else:
                self.log.info(
                    f"Creating producer for {component}: "
                    f"[add_ack: {add_ack}] "
                    f"[commands: {commands}] "
                    f"[events: {events}] "
                    f"[telemetry: {telemetry}] "
                )

                self.producers.append(
                    salkafka.ComponentProducer(
                        domain=domain,
                        name=component,
                        kafka_info=kafka_info,
                        add_ack=add_ack,
                        commands=commands,
                        events=events,
                        telemetry=telemetry,
                    )
                )

            loop = asyncio.get_running_loop()
            for s in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
                loop.add_signal_handler(s, self.signal_handler)

            try:
                self.log.info("Waiting for producers to start")
                await asyncio.gather(
                    *[producer.start_task for producer in self.producers]
                )
                self.log.info("Running")
                self.semaphore_file.touch()
                await self.wait_forever_task
            except asyncio.CancelledError:
                self.log.info("Shutting down")
                for producer in self.producers:
                    await producer.close()

        if self.semaphore_file.exists():
            self.semaphore_file.unlink()

        self.log.info("Done")

    def signal_handler(self):
        self.wait_forever_task.cancel()

    @classmethod
    async def amain(cls):
        """Parse command line arguments, then create and run a
        `ComponentProducerSet`.
        """
        parser = cls.make_argument_parser()
        args = parser.parse_args()
        if args.wait_for_ack != "all":
            args.wait_for_ack = int(args.wait_for_ack)

        if args.file is None:
            producer_set = ComponentProducerSet(args)
            await producer_set.done_task
        else:
            component_info = cls.validate(args.file)
            loop = asyncio.get_running_loop()
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=len(component_info["topic_sets"]) + 1
            ) as pool:
                producer_set = []
                for topic_set in component_info["topic_sets"]:
                    producer_set.append(
                        loop.run_in_executor(
                            pool,
                            functools.partial(
                                cls.create_process,
                                args=args,
                                component=component_info["component"],
                                add_ack=topic_set.get("add_ack", False),
                                commands=topic_set.get("commands", []),
                                events=topic_set.get("events", []),
                                telemetry=topic_set.get("telemetry", []),
                            ),
                        )
                    )
                await asyncio.gather(*producer_set)

    @classmethod
    def create_process(cls, args, component, add_ack, commands, events, telemetry):
        loop = asyncio.new_event_loop()

        asyncio.set_event_loop(loop)

        producer_set = ComponentProducerSet(
            args,
            component=component,
            add_ack=add_ack,
            commands=commands,
            events=events,
            telemetry=telemetry,
        )
        loop.run_until_complete(producer_set.done_task)

    @staticmethod
    def make_argument_parser():
        """Make a command-line argument parser.
        """
        parser = argparse.ArgumentParser(
            description="Send DDS messages to Kafka for one or more SAL components"
        )
        parser.add_argument(
            "components", nargs="*", help='Names of SAL components, e.g. "Test"',
        )
        parser.add_argument(
            "--broker",
            dest="broker_url",
            required=True,
            help="Kafka broker URL, without the transport. "
            "Required. Example: 'my.kafka:9000'",
        )
        parser.add_argument(
            "--registry",
            dest="registry_url",
            required=True,
            help="Schema Registry URL, including the transport. "
            "Required. Example: 'https://registry.my.kafka/'",
        )
        parser.add_argument(
            "--partitions",
            type=int,
            default=1,
            help="Number of partitions for each Kafka topic.",
        )
        parser.add_argument(
            "--replication-factor",
            type=int,
            default=3,
            dest="replication_factor",
            help="Number of replicas for each Kafka partition.",
        )
        parser.add_argument(
            "--loglevel",
            type=int,
            default=logging.INFO,
            help="Logging level; INFO=20 (default), DEBUG=10",
        )
        parser.add_argument(
            "--wait-ack",
            choices=("0", "1", "all"),
            default=1,
            dest="wait_for_ack",
            help="0: do not wait for ack from any Kafka broker (unsafe). "
            "1: wait for ack from one Kafka broker (default). "
            "all: wait for ack from all Kafka brokers.",
        )
        parser.add_argument(
            "--file",
            dest="file",
            required=False,
            help="Input file with component configuration in yaml format. "
            "This allows users to specify, a component and individual topics "
            "for the producers.",
        )
        parser.add_argument(
            "--show-schema",
            dest="show_schema",
            required=False,
            action="store_true",
            help="Show schema for the input file and exit.",
        )
        parser.add_argument(
            "--validate",
            dest="validate",
            required=False,
            action="store_true",
            help="Validate input file and exit.",
        )

        return parser

    @staticmethod
    def validate(filename):
        """Load and validate input file.

        Parameters
        ----------
        filename : `str`
            Name of the file to load. Must be yaml file.

        Returns
        -------
        components_info : `dict`
            Dictionary with parsed data.

        """
        schema = yaml.safe_load(
            """
        $schema: http://json-schema.org/draft-07/schema#
        $id: https://github.com/lsst-ts/ts_salkafka/salkafka.yaml
        description: Configuration for the salkafka producer.
        type: object
        additionalProperties: false
        properties:
          component:
            description: Name of SAL components, e.g. "Test".
            type: string
          topic_sets:
            type: array
            items:
              type: object
              properties:
                add_ack:
                  description: Add command acknowledgements?
                  type: boolean
                commands:
                  description: >-
                    List of commands to add to producer. To add all set it to
                    "*".
                  anyOf:
                    - type: array
                      items:
                        type: string
                    - type: string
                events:
                  description: >-
                    List of events to add to producer. To add all set it to
                    "*".
                  anyOf:
                    - type: array
                      items:
                        type: string
                    - type: string
                telemetry:
                  description: >-
                    List of telemtry to add to producer. To add all set it to
                    "*".
                  anyOf:
                    - type: array
                      items:
                        type: string
                    - type: string
        required:
          - component
        """
        )
        # First step is to validate the input schema.
        validator = jsonschema.Draft7Validator(schema)

        with open(filename) as fp:
            components_info = yaml.safe_load(fp)

        validator.validate(components_info)

        # TODO: If file schema is valid, now we need to parse the content and
        # organize the set of producers.

        return components_info
