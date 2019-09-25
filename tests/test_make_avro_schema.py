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
import unittest

import numpy as np

from lsst.ts import salobj
from lsst.ts import salkafka

index_gen = salobj.index_generator()


class MakeAvroSchemaTestCase(unittest.TestCase):
    def setUp(self):
        salobj.set_random_lsst_dds_domain()
        np.random.seed(47)

    def test_arrays(self):
        """Test the arrays event for the Test SAL component.
        """
        async def doit():
            index = next(index_gen)
            async with salobj.Domain() as domain:
                salinfo = salobj.SalInfo(domain=domain, name="Test", index=index)
                topic0 = salobj.topics.ControllerEvent(salinfo=salinfo, name="summaryState")
                schema0 = salkafka.make_avro_schema(topic=topic0)
                print(f"schema0={schema0}")

                topic = salobj.topics.ControllerEvent(salinfo=salinfo, name="arrays")
                schema = salkafka.make_avro_schema(topic=topic)
                self.assertEqual(len(schema), 3)
                self.assertEqual(schema["type"], "record")
                self.assertEqual(schema["name"], f"lsst.sal.{salinfo.name}.{topic.sal_name}")
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
                    "private_host": "long",
                    "private_revCode": "string",
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
                    # another standard field not in the XML
                    "priority": "long",
                }
                desired_fields = []
                for name, dtype in desired_field_name_type.items():
                    if name.endswith("0") and name != "char0":
                        # user-defined fields no char0; all these are arrays
                        desired_field = {"name": name,
                                         "type": {"type": "array", "items": dtype}}
                    else:
                        # standard field or char0; none of these are arrays
                        desired_field = {"name": name, "type": dtype}
                    if name == "private_sndStamp":
                        desired_field["aliases"] = ["private_efdStamp"]
                    desired_fields.append(desired_field)
                print("DESIRED")
                for field in desired_fields:
                    print(field)
                print("ACTUAL")
                for field in schema["fields"]:
                    print(field)
                self.assertEqual(schema["fields"], desired_fields)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_scalars(self):
        """Test the scalars event for the Test SAL component.
        """
        async def doit():
            index = next(index_gen)
            async with salobj.Domain() as domain:
                salinfo = salobj.SalInfo(domain=domain, name="Test", index=index)
                topic = salobj.topics.ControllerEvent(salinfo=salinfo, name="scalars")
                schema = salkafka.make_avro_schema(topic=topic)
                self.assertEqual(len(schema), 3)
                self.assertEqual(schema["type"], "record")
                self.assertEqual(schema["name"], f"lsst.sal.{salinfo.name}.{topic.sal_name}")
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
                    "private_host": "long",
                    "private_revCode": "string",
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
                desired_fields = []
                for name, dtype in desired_field_name_type.items():
                    desired_field = {"name": name, "type": dtype}
                    if name == "private_sndStamp":
                        desired_field["aliases"] = ["private_efdStamp"]
                    desired_fields.append(desired_field)
                self.assertEqual(schema["fields"], desired_fields)

        asyncio.get_event_loop().run_until_complete(doit())


if __name__ == "__main__":
    unittest.main()
