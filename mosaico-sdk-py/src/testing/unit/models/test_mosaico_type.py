from enum import Enum
from typing import Optional

import pyarrow as pa

from mosaicolabs import IMU, Image, MosaicoType, Pose, RobotPath, Serializable
from mosaicolabs.models.core.base_model import BaseModel
from mosaicolabs.models.core.serializable import MOSAICO_TYPE_KEY


class Color(str, Enum):
    RED = "red"


class Plain(BaseModel):
    x: float


def entry(field: pa.Field) -> bytes | None:
    return (field.metadata or {}).get(MOSAICO_TYPE_KEY)


def item(field: pa.Field) -> pa.Field:
    return field.type.value_field


def test_ontology_reports_its_tag():
    class Test_ontology_entry(Serializable):
        pose: Pose

    assert entry(Test_ontology_entry.__msco_pyarrow_struct__.field("pose")) == b"Pose"


def test_enum_reports_its_class_name():
    class Test_enum_entry(Serializable):
        color: Color

    assert entry(Test_enum_entry.__msco_pyarrow_struct__.field("color")) == b"Color"


def test_optional_and_annotated_are_unwrapped():
    class Test_unwrapped_entry(Serializable):
        pose: Optional[Pose] = None
        poses: Optional[MosaicoType.list_(Pose)] = None

    struct = Test_unwrapped_entry.__msco_pyarrow_struct__
    assert entry(struct.field("pose")) == b"Pose"
    assert entry(struct.field("poses")) is None
    assert entry(item(struct.field("poses"))) == b"Pose"


def test_list_reports_on_its_element():
    class Test_list_entry(Serializable):
        poses: list[Pose]
        fixed_poses: MosaicoType.list_(Pose, list_size=3)
        colors: list[Color]

    struct = Test_list_entry.__msco_pyarrow_struct__
    for name, element in [
        ("poses", b"Pose"),
        ("fixed_poses", b"Pose"),
        ("colors", b"Color"),
    ]:
        field = struct.field(name)
        assert entry(field) is None, name
        assert entry(item(field)) == element, name
        # The element is rebuilt, so check it kept its name and nullability.
        assert (item(field).name, item(field).nullable) == ("item", True), name
    assert struct.field("fixed_poses").type.list_size == 3


def test_nested_lists_report_on_the_innermost_element():
    class Test_nested_list_entry(Serializable):
        matrix: MosaicoType.matrix(Pose, rows=2, cols=3)

    outer = Test_nested_list_entry.__msco_pyarrow_struct__.field("matrix")
    inner = item(outer)
    assert (entry(outer), entry(inner), entry(item(inner))) == (None, None, b"Pose")
    assert (outer.type.list_size, inner.type.list_size) == (2, 3)


def test_other_fields_report_nothing():
    # mosaicod derives these from the Arrow type.
    class Test_no_entry(Serializable):
        alias: MosaicoType.float32
        primitive: int
        floats: list[float]
        mapping: dict[str, int]
        plain: Plain
        stamp: MosaicoType.annotate(int, pa.timestamp("us"))

    struct = Test_no_entry.__msco_pyarrow_struct__
    for field in struct:
        assert entry(field) is None, field.name
    assert entry(item(struct.field("floats"))) is None
    assert entry(struct.field("plain").type.field("x")) is None


def test_shipped_ontologies():
    imu = IMU.__msco_pyarrow_struct__
    acceleration = imu.field("acceleration")
    assert entry(acceleration) == b"Vector3d"
    assert b"description" in acceleration.metadata
    assert entry(acceleration.type.field("x")) is None
    assert entry(imu.field("header")) == b"Header"
    assert entry(imu.field("header").type.field("timestamp")) == b"Time"

    assert entry(Image.__msco_pyarrow_struct__.field("format")) == b"ImageFormat"

    poses = RobotPath.__msco_pyarrow_struct__.field("poses")
    assert entry(poses) is None
    assert entry(item(poses)) == b"Pose"
