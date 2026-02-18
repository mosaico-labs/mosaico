"""
Robot State Module.

Defines the `RobotJoint` model for capturing the state (position, velocity, effort)
of a robot's actuators.
"""

from typing import List
import pyarrow as pa

from ..mixins import HeaderMixin
from ..serializable import Serializable


class RobotJoint(Serializable, HeaderMixin):
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

    # TODO: maybe can be ok to define a type that contains the embedding of these
    # fields, and jusyt define a container

    # --- Schema Definition ---
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "names",
                pa.list_(pa.string()),
                nullable=False,
                metadata={"description": ("Names of the different robot joints")},
            ),
            pa.field(
                "positions",
                pa.list_(pa.float64()),
                nullable=False,
                metadata={
                    "description": (
                        "Positions ([rad] or [m]) of the different robot joints"
                    )
                },
            ),
            pa.field(
                "velocities",
                pa.list_(pa.float64()),
                nullable=False,
                metadata={
                    "description": (
                        "Velocities ([rad/s] or [m/s]) of the different robot joints"
                    )
                },
            ),
            pa.field(
                "efforts",
                pa.list_(pa.float64()),
                nullable=False,
                metadata={
                    "description": (
                        "Efforts ([N] or [N/m]) applied to the different robot joints"
                    )
                },
            ),
        ]
    )

    names: List[str]
    """
    Names of the different robot joints

    ### Querying with the **`.Q` Proxy**
    The names are not queryable via the `.Q` proxy (Lists are not supported yet).
    """

    positions: List[float]
    """
    Positions ([rad] or [m]) of the different robot joints

    ### Querying with the **`.Q` Proxy**
    The positions are not queryable via the `.Q` proxy (Lists are not supported yet).
    """

    velocities: List[float]
    """
    Velocities ([rad/s] or [m/s]) of the different robot joints

    ### Querying with the **`.Q` Proxy**
    The velocities are not queryable via the `.Q` proxy (Lists are not supported yet).
    """

    efforts: List[float]
    """
    Efforts ([N] or [N/m]) applied to the different robot joints

    ### Querying with the **`.Q` Proxy**
    The efforts are not queryable via the `.Q` proxy (Lists are not supported yet).
    """
