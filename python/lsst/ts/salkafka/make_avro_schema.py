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


# dict of scalar type: Avro name
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
    topic : 'lsst.ts.salobj.topics.BaseTopic`
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

    fields = []
    for field_name, field_data in data_dict.items():
        field_type = type(field_data)
        if isinstance(field_data, list):
            # field is an array
            item_type = type(field_data[0])
            item_type_name = _SCALAR_TYPE_DICT[item_type]
            field_entry = {
                "name": field_name,
                "type": {"type": "array", "items": item_type_name}
            }
        else:
            # field is a scalar
            field_type_name = _SCALAR_TYPE_DICT[field_type]
            field_entry = {
                "name": field_name,
                "type": field_type_name,
            }
        fields.append(field_entry)
    return {
        "name": f"lsst.sal.{topic.salinfo.name}.{topic.sal_name}",
        "type": "record",
        "fields": fields,
    }
