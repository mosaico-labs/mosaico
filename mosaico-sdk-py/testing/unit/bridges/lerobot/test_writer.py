import pyarrow as pa
import pytest

from mosaicolabs.bridges.lerobot.writer import _to_task_index_column

TASKS = ["Pick up the cube", "Place the cube"]


def _timeseries(task: pa.Array) -> pa.RecordBatch:
    """A batch of the timeseries topic, with its `task` field."""
    return pa.record_batch({"action": pa.array([1.0] * len(task)), "task": task})


# --- _to_task_index_column ---


@pytest.mark.parametrize("task_type", [pa.string(), pa.string_view()])
def test_task_index_column_is_the_position_of_each_task(task_type):
    """As LeRobot reads it: the row of the task in `meta/tasks.parquet`. Mosaico can stream
    the tasks as a `string_view`."""
    task = pa.array([TASKS[1], TASKS[0], TASKS[1]], task_type)

    task_index = _to_task_index_column(_timeseries(task), TASKS)

    assert task_index.to_pylist() == [1, 0, 1]


@pytest.mark.parametrize("unknown_task", ["Push the cube", None])
def test_task_index_column_rejects_a_task_not_in_the_tasks(unknown_task):
    task = pa.array([TASKS[0], unknown_task], pa.string())

    with pytest.raises(ValueError, match="not defined in the exported dataset"):
        _to_task_index_column(_timeseries(task), TASKS)


def test_task_index_column_requires_the_task_field():
    timeseries = pa.record_batch({"action": pa.array([1.0])})

    with pytest.raises(ValueError, match="has no field 'task'"):
        _to_task_index_column(timeseries, TASKS)
