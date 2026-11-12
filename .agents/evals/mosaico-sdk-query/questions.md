# Questions

Answer each one with a Python script that uses `mosaicolabs` against the daemon, and print the answer.

End the output of each script with a line `ANSWER:` followed by one item per line: a sequence name; a
topic as `sequence topic`; a time window as `start end` in nanoseconds.

1. Which recordings are rated "Very Hard"?
2. Which sequences have a declared top speed (`vmax_ms`) above 20 m/s?
3. List the sequences whose name starts with "indoor".
4. Which sequences contain an event camera, that is a topic of type `EventArray`? Give the topic too.
5. List every IMU topic in the catalog, with the sequence it belongs to.
6. In which sequences did the IMU measure an acceleration on the z axis above 25 m/s²?
7. In `race_1`, in which time windows did the IMU acceleration on z exceed 20 m/s²? Merge samples less
   than 100 ms apart into one window, and give start and end in nanoseconds.
8. Which sequences have both an IMU acceleration on x above 12 m/s² and a ground-truth pose topic
   (`/groundtruth/pose`)?
9. In which sequences did the odometry record a linear velocity along x above 30 m/s?
10. Among the sequences whose section contains "Track", which have a declared duration (`duration_s`)
    below 29 seconds?
