# Questions

Answer each one with a Python script that uses `mosaicolabs` against the daemon. Print the answer at the
end of the output: a line `ANSWER:` followed by one `key: value` line per requested key (repeat the key
for a list). Timestamps are integers in nanoseconds.

A "grid at F Hz" below always means: the first tick is the earliest sample of the requested topics, tick
`k` is at `first + round(k × 10⁹ / F)` ns, and the last tick is at or before the last sample. Rows of the
grid are numbered from 0.

1. Load the whole IMU of `race_1` (`/camera/imu`) into a pandas DataFrame. How many rows does it have,
   and what are the exact names of its three linear-acceleration columns (x, y, z)?
   Keys: `rows`, `column` (three lines).
2. Load the odometry of `race_1` (`/camera/odom/sample`) from 1654544056462859631 to
   1654544061464195967, both ends included. Give the number of rows, the first and the last timestamp, and
   the mean linear velocity along x. Keys: `rows`, `first_ns`, `last_ns`, `mean_vx`.
3. Give me the IMU linear acceleration z and the odometry linear velocity x of `race_1` aligned on a
   100 Hz grid in one DataFrame, each column holding its last known value. How many rows, and on row 1500
   what are the timestamp and the two values? Keys: `rows`, `tick_ns`, `acc_z`, `vel_x`.
4. Resample the odometry linear velocity x of `race_2` on a 100 Hz grid, keeping a value on a row only
   when the latest reading is at most 20 ms old (age ≤ 20 ms); a NaN reading is not a reading. How many
   rows, how many of them are empty, and what is the timestamp of the first empty row?
   Keys: `rows`, `empty`, `first_empty_ns`.
5. Upsample the IMU linear acceleration x of `race_1` to a 300 Hz grid without repeating any sample: a
   row gets a value only when an IMU sample arrived in the 3333333 ns before it (window start excluded,
   the tick itself included), otherwise it stays empty. How many rows, how many with a value, how many
   empty? Keys: `rows`, `with_value`, `empty`.
6. Build a scikit-learn pipeline for the IMU linear acceleration x, y, z of `race_1`: resample on a
   50 Hz grid holding the last value, then a `StandardScaler` fitted once on all the resampled rows of
   the sequence, with the timestamp left out of the features. How many rows go into the scaler, and what
   are the scaler's mean and scale for acceleration z? Keys: `rows`, `mean_acc_z`, `scale_acc_z`.
7. The recording `indoor_forward_3_davis_with_gt` is long: stream it with the extractor's default 5 s
   windows, not in one load. Build a 20 Hz grid with the IMU acceleration x (`/dvs/imu`) and the
   ground-truth position x (`/groundtruth/pose`), each column holding its last known value. How many rows,
   how many have no ground-truth position, and what is the timestamp of the first row that has one?
   Keys: `rows`, `empty_gt`, `first_gt_ns`.
