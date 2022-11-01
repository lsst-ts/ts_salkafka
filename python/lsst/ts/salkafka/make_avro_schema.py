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

__all__ = ["make_avro_schema"]

# Dict of python scalar type: Avro type name.
_SCALAR_TYPE_DICT = {
    bool: "boolean",
    int: "long",
    float: "double",
    str: "string",
}


def make_avro_schema(topic):
    """Make an Avro schema for a given topic.

    Parameters
    ----------
    topic : `lsst.ts.salobj.topics.BaseTopic`
        Topic for which to get the schema.

    Returns
    -------
    schema : `dict`
        Avro schema.

    Notes
    -----
    Scalar type names supported by Avro:

    * null    A type having no value.
    * boolean
    * int     32-bit signed integer.
    * long    64-bit signed integer.
    * float   Single precision (32-bit) IEEE 754 floating-point number.
    * double  Double precision (64-bit) IEEE 754 floating-point number.
    * bytes   Sequence of 8-bit unsigned bytes.
    * string  Sequence of unicode characters.

    Initially this function only supports ``long``, ``double``, ``string``.
    When this function is modified to parse the IDL file then it will
    support all types except ``null``.
    """
    data = topic.DataType()
    data_dict = data.get_vars()
    topic_metadata = topic.metadata

    fields = [
        dict(
            name="private_efdStamp",
            type="double",
            description="UTC time for EFD timestamp. "
            "An integer (the number of leap seconds) "
            "different from private_sndStamp.",
            units="second",
            default=0.0,
        ),
        dict(
            name="private_kafkaStamp",
            type="double",
            description="TAI time at which the Kafka message was created.",
            units="second",
            default=0.0,
        ),
    ]
    for field_name, field_data in data_dict.items():
        # Set Avro type from Python type because this is more robust than
        # getting it from field metadata (which is parsed on a "best effort"
        # basis). The cost is that some Avro field types are longer than
        # necessary (e.g. float is double and int is long).
        if isinstance(field_data, list):
            # Field is an array.
            python_item_type = type(field_data[0])
            avro_item_type = _SCALAR_TYPE_DICT[python_item_type]
            avro_field_type = dict(type="array", items=avro_item_type)
        else:
            # Field is a scalar.
            python_type = type(field_data)
            avro_field_type = _SCALAR_TYPE_DICT[python_type]
        field_entry = dict(
            name=field_name,
            type=avro_field_type,
            default=field_data,
        )

        # Add description and units metadata, if available.
        field_metadata = topic_metadata.field_info.get(field_name)
        if field_metadata is not None:
            for attr_name in ("description", "units"):
                value = getattr(field_metadata, attr_name, None)
                if value is not None:
                    field_entry[attr_name] = value

        fields.append(field_entry)

    avro_schema = dict(
        name=f"lsst.sal.{topic.salinfo.name}.{topic.sal_name}",
        type="record",
        fields=fields,
    )

    for attr_name in ("sal_version", "xml_version"):
        value = getattr(topic.salinfo.metadata, attr_name, None)
        if value is not None:
            avro_schema[attr_name] = value

    if topic_metadata.description is not None:
        avro_schema["description"] = topic_metadata.description

    return avro_schema
