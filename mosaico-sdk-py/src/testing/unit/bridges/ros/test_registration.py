import subprocess
import sys
import textwrap


def test_ros_ontology_registration():
    code = textwrap.dedent(
        # Generate a fresh environmnent
        """
            from mosaicolabs.models.core import Serializable

            _FUTURES_TAGS = [
                "BatteryState",
                "FrameTransform",
                "PointCloud2",
                "PointField",
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
