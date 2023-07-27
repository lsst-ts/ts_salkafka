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
from lsst.ts import salkafka, salobj, utils

index_gen = utils.index_generator()


class MakeAvroSchemaTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        salobj.set_random_lsst_dds_partition_prefix()
        np.random.seed(47)

    async def test_arrays(self):
        """Test the arrays event for the Test SAL component."""
        await self.check_schema(is_array=True)

    async def test_scalars(self):
        """Test the scalars event for the Test SAL component."""
        await self.check_schema(is_array=False)

    async def check_schema(self, is_array):
        """Check schema for the arrays or scalars Test event topic.

        These are almost identical, except that arrays is missing the
        string fields (because SAL does not support arrays of strings)
        """
        topic_name = "arrays" if is_array else "scalars"
        index = next(index_gen)
        async with salobj.Domain() as domain:
            salinfo = salobj.SalInfo(domain=domain, name="Test", index=index)
            topic = salobj.topics.ControllerEvent(salinfo=salinfo, name=topic_name)
            topic_sample = topic.DataType()
            schema = salkafka.make_avro_schema(topic=topic)
            assert len(schema) >= 3
            assert schema["name"] == f"lsst.sal.{salinfo.name}.{topic.sal_name}"
            assert schema["type"] == "record"
            desired_field_name_type = {
                # added by make_avro_schema
                "private_efdStamp": "double",
                "private_kafkaStamp": "double",
                # standard fields not in the XML
                "salIndex": "long",
                "private_revCode": "string",
                "private_sndStamp": "double",
                "private_rcvStamp": "double",
                "private_seqNum": "long",
                "private_origin": "long",
                "private_identity": "string",
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
            }
            if hasattr(topic_sample, "private_host"):
                # Deprecated, should be gone in ts_sal 6
                desired_field_name_type["private_host"] = "long"
            if is_array:
                for field_name in ("char0", "string0"):
                    del desired_field_name_type[field_name]

            for schema_item in schema["fields"]:
                with self.subTest(schema_item=schema_item):
                    field_name = schema_item["name"]
                    if field_name in ("private_efdStamp", "private_kafkaStamp"):
                        # These two fields are not part of the DDS topic
                        desired_default = 0
                    else:
                        desired_default = getattr(topic_sample, field_name)
                    if is_array and field_name.endswith("0"):
                        desired_item_type = desired_field_name_type[field_name]
                        desired_type = dict(type="array", items=desired_item_type)
                        if desired_item_type in ("long", "double"):
                            assert desired_default == [0] * len(desired_default)
                        elif desired_item_type == "boolean":
                            assert desired_default == [False] * len(desired_default)
                        else:
                            self.fail(
                                f"Unknown Kafka array element type {desired_item_type!r}"
                            )
                    else:
                        desired_type = desired_field_name_type[field_name]
                        if desired_type in ("long", "double"):
                            assert desired_default == 0
                        elif desired_type == "boolean":
                            assert desired_default is False
                        elif desired_type == "string":
                            assert desired_default == ""
                        else:
                            self.fail(
                                f"Unknown Kafka scalar field type {desired_type!r}"
                            )
                    assert schema_item["type"] == desired_type
                    assert schema_item["default"] == desired_default
                    if field_name == "private_efdStamp":
                        assert schema_item["units"] == "second"
                        assert schema_item["description"].startswith("UTC time for EFD")
                    if field_name == "private_kafkaStamp":
                        assert schema_item["units"] == "second"
                        assert (
                            schema_item["description"]
                            == "TAI time at which the Kafka message was created."
                        )
                    elif field_name == "salIndex":
                        # SAL 4.0 provides no metadata for this topic
                        # but SAL 4.1 may.
                        pass
                    else:
                        assert isinstance(schema_item["units"], str)
                        assert isinstance(schema_item["description"], str)
