from typing import Optional

from mosaicolabs.models import MosaicoType, Serializable
from mosaicolabs.models.types import MosaicoField


class BatteryState(Serializable):
    """
    Represents the state of a battery power supply.

    modeled after: [sensor_msgs/msg/BatteryState](https://docs.ros2.org/foxy/api/sensor_msgs/msg/BatteryState.html)

    Note:
        This model is still not included in the default ontology of Mosaico and is defined specifically for the ros-bridge module
    """

    # Core Metrics
    voltage: MosaicoType.float32 = MosaicoField(description="The battery voltage in V.")
    """The battery voltage value"""

    temperature: Optional[MosaicoType.float32] = MosaicoField(
        default=None, description="The battery temperature in °C"
    )
    """The optional battery temperature in °C"""

    current: Optional[MosaicoType.float32] = MosaicoField(
        default=None, description="Optional battery charge in A."
    )
    """The optional battery current in A"""

    charge: Optional[MosaicoType.float32] = MosaicoField(
        default=None, description="Optional battery charge in Ah."
    )
    """The optional battery charge in Ah"""

    capacity: Optional[MosaicoType.float32] = MosaicoField(
        default=None, description="Optional batterty capacity in Ah."
    )
    """The optional battery capacity in Ah"""

    design_capacity: Optional[MosaicoType.float32] = MosaicoField(
        default=None, description="Optional battery design capacity in Ah."
    )
    """The optional battery design capacity in Ah"""

    percentage: MosaicoType.float32 = MosaicoField(
        description="Battery percentage in %."
    )
    """The battery percentage in %"""

    # Status
    power_supply_status: MosaicoType.uint8 = MosaicoField(
        description="The charging status."
    )
    """The charging status"""

    power_supply_health: MosaicoType.uint8 = MosaicoField(
        description="The battery health."
    )
    """The battery health"""

    power_supply_technology: MosaicoType.uint8 = MosaicoField(
        description="The battery technology."
    )
    """The battery technology"""

    present: MosaicoType.bool = MosaicoField(description="Battery presence.")
    """The battery presence"""

    # Metadata
    location: MosaicoType.string = MosaicoField(
        description="Battery location (like the slot)."
    )
    """The battery location (like the slot)"""

    serial_number: MosaicoType.string = MosaicoField(
        description="Battery serial number."
    )
    """The battery serial number"""

    # Cell Details (Optional because some drivers don't report them)
    cell_voltage: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="Battery cells voltage."
    )
    """The battery cells voltage"""

    cell_temperature: Optional[MosaicoType.list_(MosaicoType.float32)] = MosaicoField(
        default=None, description="Battery cells temperature."
    )
    """The battery cells temperature"""
