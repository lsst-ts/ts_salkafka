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

import aiohttp
import argparse
import asyncio
import logging

from kafkit.registry.aiohttp import RegistryApi

from lsst.ts import salobj
from lsst.ts import salkafka


async def amain():
    parser = argparse.ArgumentParser(description="Send DDS messages to Kafka for one or more SAL components")
    parser.add_argument("components", nargs="+",
                        help="Names of SAL components, e.g. \"Test\"")
    parser.add_argument("--broker", dest="broker_url",
                        help="Kafka broker URL, without the transport. "
                        "Required! Example: 'my.kafka:9000'")
    parser.add_argument("--registry", dest="registry_url",
                        help="Schema Registry URL, including the transport. "
                        "Required! Example: 'https://registry.my.kafka/'")
    parser.add_argument("--wait-ack", dest="wait_for_ack", type=int,
                        default=1,
                        help="0: do not wait for ack from any kafka broker (unsafe). "
                        "1: wait for ack from one kafka broker (default). "
                        "2: wait for ack from all kafka brokers.")
    args = parser.parse_args()
    if args.broker_url is None or args.registry_url is None:
        parser.error("--broker and --registry are both required")

    log = logging.getLogger()
    log.addHandler(logging.StreamHandler())
    log.setLevel(logging.INFO)

    connector = aiohttp.TCPConnector(limit_per_host=20)
    log.info("Create domain and client session")
    async with salobj.Domain() as domain, \
            aiohttp.ClientSession(connector=connector) as httpsession:
        schema_registry = RegistryApi(session=httpsession, url=args.registry_url)

        log.info("Create producers")
        producers = []
        for name in args.components:
            producers.append(salkafka.ComponentProducer(domain=domain,
                                                        name=name,
                                                        schema_registry=schema_registry,
                                                        broker_url=args.broker_url,
                                                        wait_for_ack=args.wait_for_ack,
                                                        log=log))
        log.info("Wait for producers to start")
        await asyncio.gather(*[producer.start_task for producer in producers])
        log.info("Running")
        await asyncio.Future()  # wait forever

asyncio.get_event_loop().run_until_complete(amain())
