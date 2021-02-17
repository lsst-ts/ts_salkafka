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

import asyncio
import subprocess
import pathlib
import unittest

import asynctest
import numpy as np

from lsst.ts import salobj
from lsst.ts import salkafka

np.random.seed(47)

# Time limit (sec) for closing a topic producer set
CLOSE_TIMEOUT = 10

# Time limit (sec) to start a component producer set,
# whether distributed or not, from the command line or directly.
START_TIMEOUT = 60


class ComponentProducerSetTestCase(asynctest.TestCase):
    def run(self, result=None):
        """Override `run` to insert mocks for every test.

        https://stackoverflow.com/a/11180583
        """
        with salkafka.mocks.insert_all_mocks():
            super().run(result)

    async def setUp(self):
        salobj.set_random_lsst_dds_partition_prefix()
        self.data_dir = pathlib.Path(__file__).parent / "data" / "topic_names_sets"

        broker_url = "test.kafka:9000"
        registry_url = "https://registry.test.kafka/"
        partitions = 2
        replication_factor = 3
        wait_for_ack = 1

        self.kafka_config = salkafka.KafkaConfiguration(
            broker_url=broker_url,
            registry_url=registry_url,
            partitions=partitions,
            replication_factor=replication_factor,
            wait_for_ack=wait_for_ack,
        )

    def get_kafka_cmdline_args(self, kafka_config):
        """Get a dict of kafka command-line configuration arguments
        from self.kafka_config
        """
        return {
            "--broker": kafka_config.broker_url,
            "--registry": kafka_config.registry_url,
            "--partitions": str(kafka_config.partitions),
            "--replication-factor": str(kafka_config.replication_factor),
            "--wait-ack": str(kafka_config.wait_for_ack),
        }

    async def test_cmdline_validate_good(self):
        good_path = self.data_dir / "good_two_partitions.yaml"
        proc = await asyncio.create_subprocess_exec(
            "run_salkafka_producer.py",
            "--file",
            str(good_path),
            "--validate",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(), timeout=START_TIMEOUT
        )
        self.assertEqual(proc.returncode, 0, msg=stderr_bytes.decode())

    async def test_cmdline_validate_bad(self):
        # Test two kinds of bad data: data that does not match the schema,
        # and data that matches the schema, but is invalid.
        # These two cases raise different exceptions; both should be handled.
        for bad_name in (
            "bad_duplicate_command.yaml",  # Matches the schema
            "bad_invalid_field.yaml",  # Does not match the schema
        ):
            bad_path = self.data_dir / bad_name
            proc = await asyncio.create_subprocess_exec(
                "run_salkafka_producer.py",
                "--file",
                str(bad_path),
                "--validate",
                stdout=subprocess.PIPE,  # Swallow the error message
                stderr=subprocess.PIPE,
            )
            await asyncio.wait_for(proc.wait(), timeout=START_TIMEOUT)
            self.assertNotEqual(proc.returncode, 0)

        # Missing --file
        proc = await asyncio.create_subprocess_exec(
            "run_salkafka_producer.py",
            "--validate",
            stdout=subprocess.PIPE,  # Swallow the error message
            stderr=subprocess.PIPE,
        )
        await asyncio.wait_for(proc.wait(), timeout=START_TIMEOUT)
        self.assertNotEqual(proc.returncode, 0)

    async def test_cmdline_show_schema(self):
        proc = await asyncio.create_subprocess_exec(
            "run_salkafka_producer.py",
            "--show-schema",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(), timeout=START_TIMEOUT
        )
        self.assertEqual(proc.returncode, 0, msg=stderr_bytes.decode())
        self.assertEqual(
            stdout_bytes.decode().strip(), str(salkafka.TopicNamesSet.schema()).strip()
        )

    async def test_cmdline_bad_kafka_config(self):
        # Missing required kafka arguments
        good_kafka_args = self.get_kafka_cmdline_args(self.kafka_config)
        for required_arg in ("--broker", "--registry"):
            bad_kafka_args = good_kafka_args.copy()
            del bad_kafka_args[required_arg]
            await self.check_bad_kafka_args(bad_kafka_args)

        # Invalid values for --partitions
        for bad_partitions in (-1, 0, "not-an-int"):
            bad_kafka_args = good_kafka_args.copy()
            bad_kafka_args["--partitions"] = str(bad_partitions)
            await self.check_bad_kafka_args(bad_kafka_args)

        # Invalid values for --wait-ack
        for bad_wait_value in (-1, 2, "other"):
            bad_kafka_args = good_kafka_args.copy()
            bad_kafka_args["--wait-ack"] = str(bad_wait_value)
            await self.check_bad_kafka_args(bad_kafka_args)

    async def check_bad_kafka_args(self, bad_kafka_args):
        """Check invalid kakfa arguments.

        Checks the command-line parser two ways, expecting both to fail:

        * Specifying components with valid components
        * specifying --file with a valid file

        Parameters
        ----------
        bad_kafka_args : `dict`
            Dict of argument name: value.
            Values are cast to str if necessary.
        """
        good_path = self.data_dir / "good_two_partitions.yaml"
        bad_kafka_arg_list = []
        for key, value in bad_kafka_args.items():
            bad_kafka_arg_list += [key, str(value)]

        print("bad_kafka_arg_list =", bad_kafka_arg_list)
        proc = await asyncio.create_subprocess_exec(
            "run_salkafka_producer.py", "Test", "Script", *bad_kafka_arg_list,
        )
        await asyncio.wait_for(proc.wait(), timeout=START_TIMEOUT)
        self.assertNotEqual(proc.returncode, 0)

        proc = await asyncio.create_subprocess_exec(
            "run_salkafka_producer.py", "--file", str(good_path), *bad_kafka_arg_list,
        )
        await asyncio.wait_for(proc.wait(), timeout=START_TIMEOUT)
        self.assertNotEqual(proc.returncode, 0)

    async def test_run_producers_good(self):
        producer_set = salkafka.ComponentProducerSet(kafka_config=self.kafka_config)
        component_names = ["Script", "Test"]
        try:
            producer_task = asyncio.create_task(
                producer_set.run_producers(components=component_names)
            )
            t0 = salobj.current_tai()
            await asyncio.wait_for(producer_set.start_task, timeout=START_TIMEOUT)
            dt = salobj.current_tai() - t0
            print(f"Duration={dt:0.1f} sec")

            self.assertEqual(len(producer_set.producers), 2)
            self.assertEqual(producer_set.producers[0].salinfo.name, "Script")
            self.assertEqual(producer_set.producers[1].salinfo.name, "Test")
            for topic_producer in producer_set.producers:
                self.assertTrue(topic_producer.start_task.done())
        finally:
            producer_set.signal_handler()
            await asyncio.wait_for(producer_task, timeout=CLOSE_TIMEOUT)

    async def test_run_and_abort_producers(self):
        producer_set = salkafka.ComponentProducerSet(kafka_config=self.kafka_config)
        component_names = ["Script", "Test"]
        try:
            producer_task = asyncio.create_task(
                producer_set.run_producers(components=component_names)
            )
            # Give the topic producers time to begin starting
            await asyncio.sleep(0.1)
        finally:
            producer_set.signal_handler()
            await asyncio.wait_for(producer_task, timeout=CLOSE_TIMEOUT)

    async def test_run_producers_bad(self):
        producer_set = salkafka.ComponentProducerSet(kafka_config=self.kafka_config)
        bad_component_names = ["Script", "Script"]
        with self.assertRaises(ValueError):
            await producer_set.run_producers(components=bad_component_names)

    async def test_run_distributed_producer(self):
        topic_names_set = salkafka.TopicNamesSet.from_file(
            self.data_dir / "good_two_partitions.yaml"
        )
        producer_set = salkafka.ComponentProducerSet(kafka_config=self.kafka_config)
        try:
            producer_task = asyncio.create_task(
                producer_set.run_distributed_producer(topic_names_set=topic_names_set)
            )
            t0 = salobj.current_tai()
            await asyncio.wait_for(producer_set.start_task, timeout=START_TIMEOUT)
            dt = salobj.current_tai() - t0
            print(f"Duration={dt:0.1f} sec")
            self.assertEqual(
                len(producer_set.producer_tasks), len(topic_names_set.topic_names_list)
            )
        finally:
            producer_set.signal_handler()
            await asyncio.wait_for(producer_task, timeout=CLOSE_TIMEOUT)

    async def test_run_and_abort_distributed_producer(self):
        topic_names_set = salkafka.TopicNamesSet.from_file(
            self.data_dir / "good_two_partitions.yaml"
        )
        producer_set = salkafka.ComponentProducerSet(kafka_config=self.kafka_config)
        try:
            producer_task = asyncio.create_task(
                producer_set.run_distributed_producer(topic_names_set=topic_names_set)
            )
            print("Created task that runs run_distributed_producer")
            # Give the sub-producers time to begin starting
            await asyncio.sleep(0.1)
        finally:
            print("Call signal handler from unit test")
            producer_set.signal_handler()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(producer_task, timeout=CLOSE_TIMEOUT)


if __name__ == "__main__":
    unittest.main()
