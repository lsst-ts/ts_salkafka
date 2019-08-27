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

__all__ = ["make_run_salkafka_producer_parser", "run_salkafka_producer"]

import argparse
import asyncio
import logging

from lsst.ts import salobj
from lsst.ts import salkafka


def make_run_salkafka_producer_parser():
    """Make an argument parser for run_salkafka_producer.

    Defining this in a function allows sphinx to document it.
    """
    parser = argparse.ArgumentParser(description="Send DDS messages to Kafka for one or more SAL components")
    parser.add_argument("components", nargs="+",
                        help="Names of SAL components, e.g. \"Test\"")
    parser.add_argument("--broker", dest="broker_url", required=True,
                        help="Kafka broker URL, without the transport. "
                        "Required. Example: 'my.kafka:9000'")
    parser.add_argument("--registry", dest="registry_url", required=True,
                        help="Schema Registry URL, including the transport. "
                        "Required. Example: 'https://registry.my.kafka/'")
    parser.add_argument("--partitions", type=int, default=1,
                        help="Number of partitions for each Kafka topic.")
    parser.add_argument("--replication-factor", type=int, default=3,
                        dest="replication_factor",
                        help="Number of replicas for each Kafka partition.")
    parser.add_argument("--loglevel", type=int, default=logging.INFO,
                        help="Logging level; INFO=20 (default), DEBUG=10")
    parser.add_argument("--wait-ack", choices=("0", "1", "all"), default=1, dest="wait_for_ack",
                        help="0: do not wait for ack from any Kafka broker (unsafe). "
                        "1: wait for ack from one Kafka broker (default). "
                        "all: wait for ack from all Kafka brokers.")
    return parser


async def run_salkafka_producer():
    """Run a SAL/Kafka producer for one or more SAL components.

    This is designed to be run from a command-line script.
    """
    parser = make_run_salkafka_producer_parser()
    args = parser.parse_args()
    if args.wait_for_ack != "all":
        args.wait_for_ack = int(args.wait_for_ack)

    log = logging.getLogger()
    log.addHandler(logging.StreamHandler())
    log.setLevel(args.loglevel)

    async with salobj.Domain() as domain, \
            salkafka.KafkaInfo(broker_url=args.broker_url,
                               registry_url=args.registry_url,
                               partitions=args.partitions,
                               replication_factor=args.replication_factor,
                               wait_for_ack=args.wait_for_ack,
                               log=log) as kafka_info:

        log.info("Creating producers")
        producers = []
        for name in args.components:
            producers.append(salkafka.ComponentProducer(domain=domain,
                                                        name=name,
                                                        kafka_info=kafka_info))
        log.info("Waiting for producers to start")
        await asyncio.gather(*[producer.start_task for producer in producers])
        log.info("Running")
        await asyncio.Future()  # wait forever