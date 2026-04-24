import subprocess
import sys
import textwrap


def test_ros_ontology_registration():
    code = textwrap.dedent(
        # Generate a fresh environmnent
        """
            from mosaicolabs.models.serializable import Serializable

            _FUTURES_TAGS = [
                "battery_state",
                "frame_transform",
                "point_cloud2",
                "point_field",
            ]

            assert all(Serializable._is_registered(tag) for tag in _FUTURES_TAGS)
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
