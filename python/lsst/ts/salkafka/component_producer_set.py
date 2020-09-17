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
import psutil
import signal
import traceback
import yaml

import concurrent.futures

from lsst.ts import idl
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
        queue_len=salobj.topics.DEFAULT_QUEUE_LEN,
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
                queue_len=queue_len,
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
        queue_len,
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
                            domain=domain,
                            name=name,
                            kafka_info=kafka_info,
                            queue_len=queue_len,
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
                        queue_len=queue_len,
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
    async def amain(cls, argv):
        """Parse command line arguments, then create and run a
        `ComponentProducerSet`.
        """
        parser = cls.make_argument_parser()
        args = parser.parse_args(argv)

        if args.show_schema:
            print(cls.schema())
            return 0

        if args.wait_for_ack != "all":
            args.wait_for_ack = int(args.wait_for_ack)

        if args.file is None:
            producer_set = ComponentProducerSet(args)
            await producer_set.done_task
        else:
            component_info = cls.validate(args.file)
            loop = asyncio.get_running_loop()
            # task to wait until process is terminated
            wait_forever_task = asyncio.Future()

            # Add handle for process termination signals
            for s in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
                loop.add_signal_handler(s, wait_forever_task.cancel)

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
                                queue_len=component_info.get(
                                    "queue_len", salobj.topics.DEFAULT_QUEUE_LEN
                                ),
                                component=component_info["component"],
                                add_ack=topic_set.get("add_ack", False),
                                commands=topic_set.get("commands", []),
                                events=topic_set.get("events", []),
                                telemetry=topic_set.get("telemetry", []),
                            ),
                        )
                    )

                # In case one of the parallel producers fails, need to cancel
                # all the others and exit. This will process any task that
                # finished and then, proceed to cancel the remaining.
                main_tasks = [wait_forever_task] + producer_set

                for completed in asyncio.as_completed(main_tasks):
                    try:
                        await completed
                    except asyncio.CancelledError:
                        print("Terminating process.")
                    except Exception:
                        traceback.print_exc()
                    finally:
                        break

                # If we get here, it means that it either received a term
                # signal or one of the child processes failed.
                # Now it need to send each remaining child process a TERM
                # signal as well, before exiting.
                # Gather information about remaining child processes and send
                # signal.

                # This is the parent process id.
                parent = psutil.Process(os.getpid())

                # Child process ids.
                children = parent.children(recursive=True)

                # Send SIGTERM regardless of what signal was received. This
                # is the safest signal.
                for process in children:
                    print(f"Killing child process {process}.")
                    process.send_signal(signal.SIGTERM)

                # Wait for processes to terminate, skip any exception
                print("Waiting for child processes to finish.")
                await asyncio.gather(*producer_set, return_exceptions=True)
                print("Done")

    @classmethod
    def create_process(
        cls, args, queue_len, component, add_ack, commands, events, telemetry
    ):
        loop = asyncio.new_event_loop()

        asyncio.set_event_loop(loop)

        producer_set = ComponentProducerSet(
            args,
            queue_len=queue_len,
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
        # First step is to validate the input schema.
        validator = jsonschema.Draft7Validator(ComponentProducerSet.schema())

        with open(filename) as fp:
            components_info = yaml.safe_load(fp)

        validator.validate(components_info)

        # Make sure there is no topic left behind. Create a set with all the
        # topics and remove them as they are added. If something is left in the
        # control set at the end, it will be added to the set.
        control_set = {"add_ack": True, "commands": [], "events": [], "telemetry": []}

        component = components_info["component"]
        topic_metadata = salobj.parse_idl(
            component, idl.get_idl_dir() / f"sal_revCoded_{component}.idl"
        )
        for topic in topic_metadata.topic_info:
            if topic.startswith("command_"):
                control_set["commands"].append(topic[8:])
            elif topic.startswith("logevent_"):
                control_set["events"].append(topic[9:])
            elif topic == "ackcmd":
                pass
            else:
                control_set["telemetry"].append(topic)

        if len(control_set["commands"]) == 0:
            control_set["add_ack"] = False

        ack_added = False
        for topic_set in components_info["topic_sets"]:
            if topic_set.get("add_ack", False) and not ack_added:
                ack_added = True
                control_set["add_ack"] = False
            elif topic_set.get("add_ack", False) and ack_added:
                raise RuntimeError("Ackcmd added multiple times.")

            for topic_type in ["commands", "events", "telemetry"]:
                for topic_name in topic_set.get(topic_type, []):
                    if topic_name in control_set[topic_type]:
                        control_set[topic_type].remove(topic_name)
                    elif topic_name not in control_set[topic_type]:
                        raise RuntimeError(
                            f"Topic {topic_name} unrecognized or already included in {topic_type}."
                        )

        if control_set["add_ack"] or any(
            [
                len(control_set[topic_type]) > 0
                for topic_type in ["commands", "events", "telemetry"]
            ]
        ):
            print(
                "Some topics where not included in the list. Adding them in post process."
            )
            components_info["topic_sets"].append(control_set)

        return components_info

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
            description: Name of SAL components, e.g. "Test".
            type: string
          queue_len:
            description: Length of the python queue on the topic readers.
            type: integer
            exclusiveMinimum: {salobj.topics.DEFAULT_QUEUE_LEN}
          topic_sets:
            type: array
            items:
              type: object
              additionalProperties: false
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
