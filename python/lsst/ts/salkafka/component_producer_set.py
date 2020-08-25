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
import logging
import pathlib
import signal

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

    def __init__(self, args):

        self.log = logging.getLogger()
        self.log.addHandler(logging.StreamHandler())
        self.log.setLevel(args.loglevel)

        self.semaphore_file = pathlib.Path("/tmp", "SALKAFKA_PRODUCER_RUNNING")
        if self.semaphore_file.exists():
            self.semaphore_file.unlink()

        self.producers = []

        # A task that ends when the service is interrupted
        self.wait_forever_task = asyncio.Future()

        self.done_task = asyncio.create_task(self.run(args))

    async def run(self, args):
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

            self.log.info("Creating producers")
            self.producers = []
            for name in args.components:
                self.producers.append(
                    salkafka.ComponentProducer(
                        domain=domain, name=name, kafka_info=kafka_info
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

        producer_set = ComponentProducerSet(args)
        await producer_set.done_task

    @staticmethod
    def make_argument_parser():
        """Make a command-line argument parser.
        """
        parser = argparse.ArgumentParser(
            description="Send DDS messages to Kafka for one or more SAL components"
        )
        parser.add_argument(
            "components", nargs="+", help='Names of SAL components, e.g. "Test"'
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
        return parser
