"""
Robot State Module.

Defines the `RobotJoint` model for capturing the state (position, velocity, effort)
of a robot's actuators.
"""

from mosaicolabs.models import MosaicoType
from mosaicolabs.models.types import MosaicoField

from ..serializable import Serializable


class RobotJoint(Serializable):
    """
    Snapshot of robot joint states.

    Arrays must be index-aligned (e.g., names[0] corresponds to positions[0]).

    Attributes:
        names: Names of the different robot joints
        positions: Positions ([rad] or [m]) of the different robot joints
        velocities: Velocities ([rad/s] or [m/s]) of the different robot joints
        efforts: Efforts ([N] or [N/m]) applied to the different robot joints

    ### Querying with the **`.Q` Proxy**
    The robot joint states cannot be queried via the `.Q` proxy.
    """

    names: MosaicoType.list_(MosaicoType.string) = MosaicoField(
        description="Names of the different robot joints"
    )
    """
    Names of the different robot joints

    ### Querying with the **`.Q` Proxy**
    The names are not queryable via the `.Q` proxy (Lists are not supported yet).
    """

    positions: MosaicoType.list_(MosaicoType.float64) = MosaicoField(
        description="Positions ([rad] or [m]) of the different robot joints"
    )
    """
    Positions ([rad] or [m]) of the different robot joints

    ### Querying with the **`.Q` Proxy**
    The positions are not queryable via the `.Q` proxy (Lists are not supported yet).
    """

    velocities: MosaicoType.list_(MosaicoType.float64) = MosaicoField(
        description="Velocities ([rad/s] or [m/s]) of the different robot joints"
    )
    """
    Velocities ([rad/s] or [m/s]) of the different robot joints

    ### Querying with the **`.Q` Proxy**
    The velocities are not queryable via the `.Q` proxy (Lists are not supported yet).
    """

    efforts: MosaicoType.list_(MosaicoType.float64) = MosaicoField(
        description="Efforts ([N] or [N/m]) applied to the different robot joints"
    )
    """
    Efforts ([N] or [N/m]) applied to the different robot joints

    ### Querying with the **`.Q` Proxy**
    The efforts are not queryable via the `.Q` proxy (Lists are not supported yet).
    """
