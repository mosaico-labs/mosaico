"""
Joy Ontology Module.

Represents joystick input state.
"""

from mosaicolabs.models import MosaicoField, MosaicoType

from ..serializable import Serializable


class Joy(Serializable):
    """
    Joystick input data.

    This class represents the state of a joystick, including axis values and button states.

    Attributes:
        axes: Continuous axis values (e.g., joystick positions).
        buttons: Discrete button states (pressed or not pressed).

    ### Querying with the **`.Q` Proxy**
    Joystick data cannot be queried via the `.Q` proxy since list fields are not supported yet.
    """

    axes: MosaicoType.list_(MosaicoType.float32) = MosaicoField(
        description="The axes measurements from a joystick."
    )
    """
    Continuous axis values of the joystick.

    ### Querying with the **`.Q` Proxy**
    The axes field is not queryable via the `.Q` proxy (lists are not supported yet).
    """

    buttons: MosaicoType.list_(MosaicoType.int32) = MosaicoField(
        description="The buttons measurements from a joystick."
    )
    """
    Discrete button states (1 = pressed, 0 = released).

    ### Querying with the **`.Q` Proxy**
    The buttons field is not queryable via the `.Q` proxy (lists are not supported yet).
    """
