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
import concurrent.futures
import functools
import logging
import multiprocessing
import os
import pathlib
import psutil
import queue
import signal
import sys
import traceback

from lsst.ts import salobj
from .topic_names_set import TopicNamesSet
from .component_producer import ComponentProducer
from .kafka_producer_factory import KafkaConfiguration, KafkaProducerFactory


def asyncio_run_func(async_func, **kwargs):
    """Call asyncio.run on a specified async function with specified keyword
    arguments.

    Designed to run a coroutine as a subprocess using run_in_executor.
    The coroutine must be created in the destination process's own event
    loop, which is a nuisance without a function such as this.

    Parameters
    ----------
    async_func : ``callable``
        Async function or other callable.
    **kwargs
        Keyword arguments for ``async_func``.

    Notes
    -----
    Here is a trivial example. Save the following to a file, along with the
    definition for this function, and run it from the command line (be sure
    (to include ``if __name__...`` to avoid errors on some systems)::

        async def trivial_wait(index, delay):
            print(f"trivial_wait(index={index}, delay={delay}) begins")
            await asyncio.sleep(delay)
            print(f"trivial_wait(index={index}, delay={delay}) ends")


        async def amain():
            loop = asyncio.get_running_loop()
            tasks = []
            with concurrent.futures.ProcessPoolExecutor() as pool:
                for index in range(3):
                    tasks.append(loop.run_in_executor(
                        pool,
                        functools.partial(
                            asyncio_run_func,
                            trivial_wait,
                            index=index,
                            delay=2,
                        ),
                    ))
            await asyncio.gather(*tasks)


        if __name__ == "__main__":
            asyncio.run(amain())
    """
    asyncio.run(async_func(**kwargs))


class ComponentProducerSet:
    r"""A collection of one or more `ComponentProducer`\ s
    created from a command-line script.

    The normal way to use this class is to run `ComponentProducerSet.amain`
    from a command-line script. If you wish to run it more directly
    (e.g. for unit tests), do one of the following:

    * To handle all topics for one or more SAL components::

        kafka_config = KafkaConfig(_kafka info_)
        producer_set = ComponentProducerSet(kafka_config=kafka_config)
        await producer_set.run_producers(components=_component names_)

    * To distribute production of one SAL component among multiple
      subprocesses::

        topic_names_set = TopicNamesSet.from_file(_filepath_)
        kafka_config = KafkaConfig(_kafka_info_)
        producer_set = ComponentProducerSet(kafka_config=kafka_config)
        await producer_set.run_distributed_producer(
            topic_names_set=topic_names_set,
        )

    Parameters
    ----------
    kafka_config : `KafkaConfig`
        Kafka configuration.
    log_level : `int`, optional
        Log level, e.g. logging.INFO.
    """

    def __init__(
        self,
        kafka_config,
        log_level=logging.INFO,
    ):
        self.kafka_config = kafka_config

        self.log = logging.getLogger()
        if not self.log.hasHandlers():
            self.log.addHandler(logging.StreamHandler())
        self.log.setLevel(log_level)

        semaphore_filename = "SALKAFKA_PRODUCER_RUNNING"
        suffix = os.environ.get("SALKAFKA_SEMAPHORE_SUFFIX")
        if suffix is not None:
            semaphore_filename += f"_{suffix.upper()}"

        self.semaphore_file = pathlib.Path("/tmp", semaphore_filename)
        if self.semaphore_file.exists():
            self.semaphore_file.unlink()

        # A collection of ComponentProducers.
        # Set by run_producers but not run_distributed_producer.
        self.producers = []

        # A collection of producer subprocess tasks.
        # Set by run_distributed_producer but not run_producers.
        self.producer_tasks = []

        self.start_task = asyncio.Future()

        # Internal task to monitor startup of producer(s)
        # This is different than start_task in order to avoid a race condition
        # when code (typically a unit test) creates a ComponentProducerSet
        # and immediately starts waiting for start_task to be done.
        self._interruptable_start_task = salobj.make_done_future()

        self._run_producer_subprocess_task = salobj.make_done_future()

        # A task that ends when the service is interrupted.
        self._wait_forever_task = asyncio.Future()

    @classmethod
    async def amain(cls):
        """Parse command line arguments and create and run a
        `ComponentProducerSet`.
        """
        parser = cls.make_argument_parser()
        args = parser.parse_args()

        if args.file is not None and len(args.components) > 0:
            parser.error("Cannot specify components and --file together; pick one.")

        if args.show_schema:
            print(TopicNamesSet.schema())
            return 0

        if args.validate:
            if args.file is None:
                parser.error("Must specify --file with --validate")
            try:
                topic_names_set = TopicNamesSet.from_file(args.file)
            except Exception as e:
                print(f"File {args.file!r} is not valid: {e}")
                sys.exit(1)

            print(f"File {args.file!r} is valid.")
            return

        # Parse Kafka configuration, but first fix the type of wait_for_ack:
        # cast the value to int, unless it is "all".
        if args.broker_url is None or args.registry_url is None:
            parser.error(
                "You must specify --broker and --registry "
                "unless you use --show-schema or --validate"
            )
        if int(args.partitions) <= 0:
            parser.error(f"--partitions={args.partitions} must be positive")
        if args.wait_for_ack != "all":
            args.wait_for_ack = int(args.wait_for_ack)
        kafka_config = KafkaConfiguration(
            broker_url=args.broker_url,
            registry_url=args.registry_url,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
            wait_for_ack=args.wait_for_ack,
        )

        if args.file is None:
            # Use a single process to handle all topics for one or more
            # SAL components.
            if len(args.components) == 0:
                parser.error(
                    "Nothing to do; specify one or more components, or --file."
                )
            producer_set = cls(kafka_config=kafka_config, log_level=args.log_level)
            await producer_set.run_producers(components=args.components)
        else:
            # Validate the file and quit if --validate,
            # else run subprocesses to handle subsets of topics
            # for one chatty SAL component.
            if len(args.components) > 0:
                parser.error("Cannot specify components and --file together; pick one.")
            topic_names_set = TopicNamesSet.from_file(args.file)

            if args.validate:
                print(f"File {args.file!r} is valid.")
                return

            producer_set = cls(kafka_config=kafka_config, log_level=args.log_level)
            await producer_set.run_distributed_producer(topic_names_set=topic_names_set)

    @classmethod
    async def create_producer_subprocess(
        cls,
        *,
        kafka_config,
        component,
        index,
        topic_names,
        log_level,
        started_queue,
        queue_len=salobj.topics.DEFAULT_QUEUE_LEN,
    ):
        """Create and run a producer for a subset of one SAL component's
        topics.

        Parameters
        ----------
        kafka_config : `KafkaConfig`
            Kafka configuration.
        component : `str`
            Name of a SAL component for which to handle a subset of topics.
        index : `int`
            Index of topic_names in TopicNamesSet;
            identifies this sub-producers.
        topic_names : `TopicNames`
            Topic names.
        log_level : `int`, optional
            Log level, e.g. logging.INFO.
        started_queue : `multiprocessing.Queue`
            A queue to which to publish the index
            when this producer has started running.
        queue_len : `int`, optional
            Length of the DDS read queue. Must be greater than or equal to
            `salobj.domain.DDS_READ_QUEUE_LEN`, which is the default.
        """
        producer_set = cls(kafka_config=kafka_config, log_level=log_level)

        producer_set._run_producer_subprocess_task = asyncio.create_task(
            producer_set.run_producer_subprocess(
                producer_set=producer_set,
                component=component,
                index=index,
                topic_names=topic_names,
                started_queue=started_queue,
                queue_len=queue_len,
            )
        )

        await producer_set._run_producer_subprocess_task

    @staticmethod
    def make_argument_parser():
        """Make a command-line argument parser."""
        parser = argparse.ArgumentParser(
            description="Send DDS messages to Kafka for one or more SAL components"
        )
        parser.add_argument(
            "components",
            nargs="*",
            help="Names of SAL components, e.g. ATDome ATDomeTrajectory. "
            "If a SAL component has multiple SAL indices, such as MTHexapod "
            "or Script, messages from all indices are read by the producer "
            "(there is no way to restrict to a subset of indices). "
            "Ignored if --file is specified.",
        )
        parser.add_argument(
            "--broker",
            dest="broker_url",
            help="Kafka broker URL, without the transport. "
            "Example 'my.kafka:9000'. "
            "Required unless --validate or --show-schema are specified.",
        )
        parser.add_argument(
            "--registry",
            dest="registry_url",
            help="Schema Registry URL, including the transport. "
            "Example: 'https://registry.my.kafka/'. "
            "Required unless --validate or --show-schema are specified. ",
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
            dest="log_level",
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
            help="File specifying how to split topics for one SAL component "
            "among multiple subprocesses. This is useful for chatty "
            "SAL components such as MTM1M3. "
            "Run with --validate to validate this file and exit. "
            "Run with --show-schema to show the schema for such files.",
        )
        parser.add_argument(
            "--show-schema",
            dest="show_schema",
            required=False,
            action="store_true",
            help="Show the schema for --file option files and exit.",
        )
        parser.add_argument(
            "--validate",
            dest="validate",
            required=False,
            action="store_true",
            help="Validate the file specified by --file and exit.",
        )

        return parser

    async def run_distributed_producer(self, topic_names_set):
        """Produce messages for one SAL component, distributing the topics
        among multiple subprocesses.

        Parameters
        ----------
        topic_names_set : `TopicNamesSet`
            Component name and topic names list.
        """
        loop = asyncio.get_running_loop()

        # Add handle for process termination signals
        for s in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            loop.add_signal_handler(s, self.signal_handler)

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=len(topic_names_set.topic_names_list)
        ) as pool:
            # One cannot simply pass multiprocessing.Queue()
            # between processes; see
            # https://stackoverflow.com/a/9928191/1653413
            started_queue = multiprocessing.Manager().Queue()
            for index, topic_names in enumerate(topic_names_set.topic_names_list):
                self.producer_tasks.append(
                    loop.run_in_executor(
                        pool,
                        functools.partial(
                            asyncio_run_func,
                            self.create_producer_subprocess,
                            kafka_config=self.kafka_config,
                            component=topic_names_set.component,
                            index=index,
                            topic_names=topic_names,
                            log_level=self.log.getEffectiveLevel(),
                            queue_len=topic_names_set.queue_len,
                            started_queue=started_queue,
                        ),
                    )
                )

            # Wait for all subprocesses to start
            print("Waiting for partial producers to start")
            try:
                self._interruptable_start_task = asyncio.create_task(
                    self.wait_partial_producers_started(
                        num_producers=len(topic_names_set.topic_names_list),
                        started_queue=started_queue,
                    )
                )
                await self._interruptable_start_task
                print("Partial producers are all running")
                self.start_task.set_result(None)

                # In case one of the parallel producers fails, need to cancel
                # all the others and exit. This will process any task that
                # finished and then, proceed to cancel the remaining.
                main_tasks = [self._wait_forever_task] + self.producer_tasks

                for completed in asyncio.as_completed(main_tasks):
                    try:
                        await completed
                    except asyncio.CancelledError:
                        print("Terminating process.")
                    except Exception:
                        traceback.print_exc()
                    finally:
                        break

            finally:
                # Time to quit. We have received a term signal or one of the
                # child processes has failed. Send each remaining child process
                # a TERM signal, then exit.
                print("Shutting down partial producers")
                main_process = psutil.Process(os.getpid())
                child_processes = main_process.children(recursive=True)

                # Send SIGTERM regardless of what signal was received, because
                # it is not safe to send SIGKILL to a process that uses DDS.
                for process in child_processes:
                    print(f"Killing child process {process.pid}.")
                    process.send_signal(signal.SIGTERM)

                # Wait for processes to terminate, on a "best effort" basis,
                # so ignore exceptions.
                print("Waiting for partial producer processes to finish.")
                await asyncio.gather(*self.producer_tasks, return_exceptions=True)
                print("Done")

    async def run_producers(
        self,
        *,
        components,
        queue_len=salobj.topics.DEFAULT_QUEUE_LEN,
    ):
        """Create and run component producers for one or more SAL components.

        This version produces all topics for each component.

        Parameters
        ----------
        components : `List` [`str`]
            Names of SAL components for which to produce Kafka messages.
        queue_len : `int`, optional
            Length of the DDS read queue. Must be greater than or equal to
            `salobj.domain.DDS_READ_QUEUE_LEN`, which is the default.

        Raises
        ------
        ValueError
            If components contains duplicate names.
        RuntimeError
            If there is no IDL file for one of the components.
        """
        components_set = set(components)
        if len(components_set) < len(components):
            raise ValueError(f"components={components} has duplicates")
        async with salobj.Domain() as domain, KafkaProducerFactory(
            config=self.kafka_config,
            log=self.log,
        ) as kafka_factory:
            self.log.info(f"Creating producers for {components}")
            self.domain = domain
            self.kafka_factory = kafka_factory

            self.producers = []
            for component in components:
                self.producers.append(
                    ComponentProducer(
                        domain=domain,
                        component=component,
                        kafka_factory=kafka_factory,
                        queue_len=queue_len,
                    )
                )

            loop = asyncio.get_running_loop()
            for s in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
                loop.add_signal_handler(s, self.signal_handler)

            try:
                if self._wait_forever_task.done():
                    self.log.warning("Cancelled before waiting for producer to start")
                else:
                    self.log.info("Waiting for producers to start")
                    self._interruptable_start_task = asyncio.gather(
                        *[producer.start_task for producer in self.producers]
                    )
                    await self._interruptable_start_task
                    self.log.info("Running")
                    self.start_task.set_result(None)
                    self.semaphore_file.touch()
                    await self._wait_forever_task
            except asyncio.CancelledError:
                self.log.info("Shutting down")
                for producer in self.producers:
                    await producer.close()

        if self.semaphore_file.exists():
            self.semaphore_file.unlink()

        self.log.info("Done")

    async def run_producer_subprocess(
        self,
        producer_set,
        component,
        index,
        topic_names,
        started_queue,
        queue_len=salobj.topics.DEFAULT_QUEUE_LEN,
    ):
        """Run a producer subprocess created by create_producer_subprocess.

        This is a separate method so it can be interrupted with
        the signal handler (which otherwise could not easily interrupt
        the creation of salobj.Domain and KafkaProducerFactory).
        """
        loop = asyncio.get_running_loop()
        for s in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            loop.add_signal_handler(s, self.signal_handler)

        async with salobj.Domain() as domain, KafkaProducerFactory(
            config=self.kafka_config,
            log=self.log,
        ) as kafka_factory:
            self.domain = domain
            self.kafka_factory = kafka_factory

            self.log.info(
                f"Creating partial producer {index} for {component}: "
                f"[add_ackcmd: {topic_names.add_ackcmd}] "
                f"[commands: {topic_names.commands}] "
                f"[events: {topic_names.events}] "
                f"[telemetry: {topic_names.telemetry}] "
            )

            self.producers = [
                ComponentProducer(
                    domain=domain,
                    component=component,
                    kafka_factory=kafka_factory,
                    queue_len=queue_len,
                    topic_names=topic_names,
                )
            ]

            try:
                if not self._wait_forever_task.done():
                    self.log.info(f"Waiting for partial producer {index} to start")
                    self._interruptable_start_task = asyncio.gather(
                        *[producer.start_task for producer in self.producers]
                    )
                    await self._interruptable_start_task
                    self.start_task.set_result(None)
                    started_queue.put(index)
                    self.log.info(f"Partial producer {index} running")
                    self.semaphore_file.touch()
                    await self._wait_forever_task
            except asyncio.CancelledError:
                self.log.info(f"Partial producer {index} shutting down")
                for producer in self.producers:
                    await producer.close()

        if self.semaphore_file.exists():
            self.semaphore_file.unlink()

        self.log.info(f"Partial producer {index} done")

    def signal_handler(self):
        print("ComponentProducerSet.signal_handler begins", file=sys.stderr)
        self._run_producer_subprocess_task.cancel()
        self._interruptable_start_task.cancel()
        self._wait_forever_task.cancel()
        print("ComponentProducerSet.signal_handler ends", file=sys.stderr)

    async def wait_partial_producers_started(self, num_producers, started_queue):
        """Wait for all partial producers to report that they have started.

        Parameters
        ----------
        num_producers : `int`
            The number of partial producers to wait for
        started_queue : `multiprocessing.Queue`
            Queue that receives started notifications,
            as the index of ach ppartial producer that has started.
        """
        to_be_started_indices = set(range(num_producers))
        while to_be_started_indices:
            try:
                index = started_queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.1)
                continue
            try:
                to_be_started_indices.remove(index)
            except KeyError:
                self.log.warning(
                    f"Partial producer {index} reported as started more than once"
                )
