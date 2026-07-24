from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.enum.session_level_error_policy import SessionLevelErrorPolicy
from mosaicolabs.models.core.message import Message
from mosaicolabs.models.futures.laser import MultiEchoLaserScan


def test_sending_MultiEchoLaserScan(mosaico_client: MosaicoClient):
    print(MultiEchoLaserScan.__msco_pyarrow_struct__)
    with mosaico_client:
        try:
            with mosaico_client.sequence_create(
                "test_multi_echo_push", {}, on_error=SessionLevelErrorPolicy.Delete
            ) as seqw:
                tw = seqw.topic_create("/multi/echo", {}, MultiEchoLaserScan)
                assert tw is not None
                model = MultiEchoLaserScan(
                    angle_min=1.0,
                    angle_max=1.0,
                    angle_increment=1.0,
                    time_increment=1.0,
                    scan_time=12.12,
                    range_min=1.0,
                    range_max=3.0,
                    ranges=[[1.0, 2.0, 3.0], [-1.0, -2.0, -3.0]],
                    intensities=[[1.0, 2.0, 3.0], [-1.0, -2.0, -3.0]],
                )
                tw.push(Message(timestamp_ns=1234567, data=model))

        except Exception:
            mosaico_client.sequence_delete("test_multi_echo_push")
            raise
        mosaico_client.sequence_delete("test_multi_echo_push")
