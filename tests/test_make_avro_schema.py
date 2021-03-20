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

import unittest

import numpy as np

from lsst.ts import salobj
from lsst.ts import salkafka

index_gen = salobj.index_generator()


class MakeAvroSchemaTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        salobj.set_random_lsst_dds_partition_prefix()
        np.random.seed(47)

    async def test_arrays(self):
        """Test the arrays event for the Test SAL component.
        """
        index = next(index_gen)
        async with salobj.Domain() as domain:
            salinfo = salobj.SalInfo(domain=domain, name="Test", index=index)
            topic = salobj.topics.ControllerEvent(salinfo=salinfo, name="arrays")
            topic_sample = topic.DataType()
            schema = salkafka.make_avro_schema(topic=topic)
            self.assertGreaterEqual(len(schema), 3)
            self.assertEqual(
                schema["name"], f"lsst.sal.{salinfo.name}.{topic.sal_name}"
            )
            self.assertEqual(schema["type"], "record")
            desired_field_name_type = {
                # Added by make_avro_schema.
                "private_kafkaStamp": "double",
                # Standard fields. These are not in the XML.
                "TestID": "long",
                "private_revCode": "string",
                "private_sndStamp": "double",
                "private_rcvStamp": "double",
                "private_seqNum": "long",
                "private_origin": "long",
                "private_host": "long",
                "private_revCode": "string",
                # This standard field is only present for events.
                "priority": "long",
                # User-defined fields; these are in the XML.
                "boolean0": "boolean",
                "byte0": "long",
                "short0": "long",
                "int0": "long",
                "long0": "long",
                "longLong0": "long",
                "octet0": "long",
                "unsignedShort0": "long",
                "unsignedInt0": "long",
                "unsignedLong0": "long",
                "float0": "double",
                "double0": "double",
            }
            if hasattr(topic_sample, "private_host"):
                # Deprecated, will be gone in ts_sal 5
                desired_field_name_type["private_host"] = "long"
            if hasattr(topic_sample, "private_identity"):
                # Coming in ts_sal 4.2
                desired_field_name_type["private_identity"] = "string"

            schema_field_names = [item["name"] for item in schema["fields"]]
            self.assertEqual(
                set(desired_field_name_type.keys()), set(schema_field_names)
            )
            for schema_item in schema["fields"]:
                with self.subTest(schema_item=schema_item):
                    field_name = schema_item["name"]
                    desired_item_type = desired_field_name_type[field_name]
                    if field_name.endswith("0") and field_name != "char0":
                        desired_type = dict(type="array", items=desired_item_type)
                    else:
                        desired_type = desired_item_type
                    self.assertEqual(schema_item["type"], desired_type)
                    if field_name == "private_kafkaStamp":
                        self.assertEqual(schema_item["units"], "second")
                        self.assertEqual(
                            schema_item["description"],
                            "TAI time at which the Kafka message was created.",
                        )
                    elif field_name == "TestID":
                        # SAL 4.0 provides no metadata for this topic
                        # but SAL 4.1 may.
                        pass
                    elif field_name.startswith("private_"):
                        self.assertIsInstance(schema_item["units"], str)
                        self.assertIsInstance(schema_item["description"], str)
                    else:
                        # SAL 4.0 provides no metadata for array topics
                        # but SAL 4.1 will.
                        pass

    async def test_scalars(self):
        """Test the scalars event for the Test SAL component.
        """
        index = next(index_gen)
        async with salobj.Domain() as domain:
            salinfo = salobj.SalInfo(domain=domain, name="Test", index=index)
            topic = salobj.topics.ControllerEvent(salinfo=salinfo, name="scalars")
            topic_sample = topic.DataType()
            schema = salkafka.make_avro_schema(topic=topic)
            self.assertGreaterEqual(len(schema), 3)
            self.assertEqual(
                schema["name"], f"lsst.sal.{salinfo.name}.{topic.sal_name}"
            )
            self.assertEqual(schema["type"], "record")
            desired_field_name_type = {
                # added by make_avro_schema
                "private_kafkaStamp": "double",
                # standard fields not in the XML
                "TestID": "long",
                "private_revCode": "string",
                "private_sndStamp": "double",
                "private_rcvStamp": "double",
                "private_seqNum": "long",
                "private_origin": "long",
                "private_revCode": "string",
                # This standard field is only present for events.
                "priority": "long",
                # fields in the XML
                "boolean0": "boolean",
                "byte0": "long",
                "char0": "string",
                "short0": "long",
                "int0": "long",
                "long0": "long",
                "longLong0": "long",
                "octet0": "long",
                "unsignedShort0": "long",
                "unsignedInt0": "long",
                "unsignedLong0": "long",
                "float0": "double",
                "double0": "double",
                "string0": "string",
                # another standard field not in the XML
                "priority": "long",
            }
            if hasattr(topic_sample, "private_host"):
                # Deprecated, will be gone in ts_sal 5
                desired_field_name_type["private_host"] = "long"
            if hasattr(topic_sample, "private_identity"):
                # Coming in ts_sal 4.2
                desired_field_name_type["private_identity"] = "string"

            for schema_item in schema["fields"]:
                with self.subTest(schema_item=schema_item):
                    field_name = schema_item["name"]
                    print(f"field_name={field_name}")
                    desired_type = desired_field_name_type[field_name]
                    self.assertEqual(schema_item["type"], desired_type)
                    if field_name == "private_kafkaStamp":
                        self.assertEqual(schema_item["units"], "second")
                        self.assertEqual(
                            schema_item["description"],
                            "TAI time at which the Kafka message was created.",
                        )
                    elif field_name == "TestID":
                        # SAL 4.0 provides no metadata for this topic
                        # but SAL 4.1 may.
                        pass
                    else:
                        self.assertIsInstance(schema_item["units"], str)
                        self.assertIsInstance(schema_item["description"], str)


if __name__ == "__main__":
    unittest.main()
