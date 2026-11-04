---
title: Retrieval
sidebar_position: 6
description: "How mosaicod streams data back to clients via the Arrow Flight Get path. Covers time-range slicing, columnar Arrow record batch streaming, and the retrieval action API."
---

Unlike simple file downloads, this protocol provides an interface for requesting precise data slices, dynamically assembled and streamed back as optimized stream of Arrow record batches.

## The Retrieval Protocol

Accessing data requires specifying the [Locator](index.md#resource-addressing), which defines the topic path, and an optional time range in nanoseconds.

Upon receiving a request, the server performs an index lookup in the metadata cache to identify physical data chunks intersecting the requested time window. 
This is followed by pruning, discarding chunks outside the query bounds to avoid redundant I/O. 
Once relevant segments are identified, the server streams the data by opening underlying files and delivering it in a high-throughput pipeline.

In the protocol, the `get_flight_info` call returns a list of resources, each containing an endpoint (the name of the topic or sequence, such as `my_sequence` or `my_sequence/my/topic`) and a ticket, an opaque binary blob used by the server in the `do_get` call to extract and stream the data. 

Calling `get_flight_info` on a sequence returns all topics associated with that sequence, whereas calling it on a specific topic returns only the endpoint and ticket for that topic.

```py title="retrieval_protocol"
# Define the locator and the temporal slice
locator = "my_sequence/topic/1"
time_range = (start_ns, end_ns)  # Optional: defaults to full history

# Resolve the locator into endpoint and tickets
flight_info = get_flight_info(locator, time_range)

for endpoint in flight_info.endpoints:
    # Use the ticket to stream the actual data batches
    data_stream = do_get(endpoint.ticket)
    for batch in data_stream:
        process(batch)
```

## Image Sequences

A topic of the `ImageSequence` (PNG) or `ImageSequenceRaw` (raw images) serialization format can be read back as its images or as its video, chosen with the `image_sequence` field of the `GetFlightInfoCmd`. The server copies it into the tickets of image sequence topics; a sequence's other topics ignore it.

```proto
message GetFlightInfoCmd {
  string locator = 1;
  time.TimestampRange timestamp_range = 2;
  ImageSequenceRequest image_sequence = 3;  // not set: the images
}

message ImageSequenceRequest {
  oneof view {
    ImagesView images = 1;  // not set: the images
    VideoView video = 2;
  }
}

message VideoView {
  VideoInterval interval = 1;  // not set: all of the video
}

message VideoInterval {  // seconds of video [start_s, end_s), 0 at the first frame
  uint32 start_s = 1;
  uint32 end_s = 2;
}
```

What can be read back depends on whether the [store optimizer](store_optimizer.md#image-sequences) has turned the images into a video yet:

| View | Before the video | Once it is a video |
| :--- | :--- | :--- |
| `images` | the images, as uploaded | the images, decoded from the video (needs a server built with the [`video` feature](install.md#the-video-feature)) |
| `video` | `FailedPrecondition` | the video as MPEG-DASH segments, one per second |

Asking for a video of a topic that is not an image sequence is also a `FailedPrecondition`, and so is asking a server built without the `video` feature for the images of a video.

The video is lossless FFV1 in a fragmented MP4 file: a header, then one fragment per frame, each at its time in microseconds from the first capture. Every frame is a keyframe. The exact timestamps of the frames, to the nanosecond, come with the images and with the segments.

### Images

The images come back with the topic's schema, the one `get_schema` returns, metadata included, sorted by `timestamp_ns`. A time range keeps those whose `timestamp_ns` is in it, as for any topic. Decoded from a video, every column but the images comes back as uploaded; raw images (`ImageSequenceRaw`) come back byte for byte, and PNGs (`ImageSequence`) with exactly the uploaded pixels, but not the uploaded bytes: the server encodes them again.

### Video

The video as [MPEG-DASH](https://www.mpeg.org/standards/MPEG-DASH/), cut without re-encoding it, a media segment per second: segment `k` holds the frames whose time in the video is in `[k, k + 1)` seconds, second 0 starting at the first frame. Seconds with no frames, in a gap of the capture, have no segment. Since every frame is a keyframe, every segment starts with one.

The `interval` picks the seconds `[start_s, end_s)`: `[0, 1)` is segment 0, `[1, 3)` segments 1 and 2. Without it, all the segments come back. A wrong interval is an `InvalidArgument`:

- `start_s` is not less than `end_s`, such as `[0, 0)` or `[2, 1)`;
- `end_s` is after the video, whose seconds go up to that of its last frame;
- the request also has a `timestamp_range`: the video takes seconds, not a time range.

The files come in this order:

- `manifest.mpd`: a static MPD, with a `SegmentTimeline` of the media segments, in microseconds;
- `init.mp4`: the initialization segment;
- `segment-<t>.m4s`: each media segment, `<t>` the time of its first frame in microseconds.

A client decodes segments by appending them to the initialization segment: the result is an MP4 video of their frames, at their time in the whole video. Appending all of them gives the whole video. The manifest is for DASH tools.

Each file comes in pieces, one row per piece:

| Column | Type | |
| :--- | :--- | :--- |
| `name` | `Utf8` | the file |
| `segment` | `UInt64` | a media segment's number `k`: its second of video |
| `start_us` | `Int64` | a media segment's second start, in microseconds of video |
| `duration_us` | `Int64` | a media segment's duration, `1000000` |
| `offset` | `UInt64` | where the piece starts in the file |
| `data` | `Binary` | the piece |
| `last` | `Boolean` | whether it is the last piece of the file |
| `timestamp_ns` | `List<Int64>` | the `timestamp_ns` of each frame of a media segment, on its first piece only |
| `capture_timestamp_ns` | `List<Int64>` | the capture time of each frame of a media segment, on its first piece only |
| `video_timestamp_us` | `List<Int64>` | the time of each frame in the video, on its first piece only |

`segment`, `start_us`, `duration_us` and the lists are null on the manifest and the initialization segment. The schema's metadata gives `mosaico:content_type` (`video/mp4`), `mosaico:codec` (`ffv1`), `mosaico:frames`, `mosaico:timescale` (`1000000`: microseconds) and `mosaico:capture_start_ns`, the capture time of the first frame, which the video's timestamps start from.

FFV1 has no registered MP4 sample entry or DASH codecs string: the segments carry it as FFmpeg writes it (`FFV1`), and the manifest names it `ffv1`. FFmpeg and tools built on it, such as PyAV, read them; browsers and most DASH players do not. Reading the manifest with FFmpeg needs a build with `libxml2`.

## Sequence List

To list every sequence available in the system, call `list_flights` with a `ListFlightsCmd`:

```py title="list_sequences"
# List all sequences in the system, oldest first
cmd = ListFlightsCmd(order_by=SEQUENCE_ORDER_CREATED_AT)
flight_info = list_flights(cmd.SerializeToString())
```

Each returned `FlightInfo` carries the sequence name in its descriptor path, which can then be used to retrieve specific topics or data slices.

### Pagination

With a positive `limit`, `list_flights` returns one page at a time. Its `Criteria.expression` carries a protobuf `ListFlightsCmd`:

```proto
message ListFlightsCmd {
  uint32 limit = 1;              // 0: no limit
  oneof start {
    SequenceOrder order_by = 2;  // first page: SEQUENCE_ORDER_CREATED_AT or SEQUENCE_ORDER_NAME
    SequenceCursor cursor = 3;   // next pages, in the cursor's order
  }
}
```

Ordering is always ascending.

Each returned `FlightInfo` also carries a `SequenceListItemAppMetadata` in `app_metadata`. Its `cursor` points just past that sequence: copy it into the next request, in place of `order_by`, to continue.

```py title="list_sequences_paged"
# First page: 20 sequences, by name
cmd = ListFlightsCmd(order_by=SEQUENCE_ORDER_NAME, limit=20)
page = list_flights(cmd.SerializeToString())

# Next page: resume right after the last sequence received
last = SequenceListItemAppMetadata.FromString(page[-1].app_metadata)
cmd.cursor.CopyFrom(last.cursor)
page = list_flights(cmd.SerializeToString())
```

- **End of the list.** A page with fewer than `limit` sequences is the last one. When the total is an exact multiple of `limit`, one more request returns an empty page. With `limit: 0`, the stream itself is the whole rest of the list.
- **Resuming.** Every sequence carries its own cursor, so a client that stops in the middle of a page can resume right after the last sequence it handled. A cursor stays valid even if the sequence it was built from has since been deleted.
- **Consistency.** The listing is live, not a snapshot. Each page reflects the database when it is fetched: deleted sequences disappear, and sequences created between requests only show up if they sort after the cursor.
- **Errors.** `InvalidArgument` is returned when the expression is not a valid `ListFlightsCmd`, or sets neither `order_by` nor `cursor`.

The Python SDK wraps this in `MosaicoClient.list_sequences_page()` and `MosaicoClient.iter_sequences()`, which fetch 50 sequences per page, oldest first, by default. `MosaicoClient.list_sequences()` returns every sequence at once.

## Metadata Context Headers

To provide full context, the data stream is prefixed with a schema message containing embedded custom metadata. Mosaico injects context into this header for client reconstruction of the environment.

This includes `user_metadata`, preserving original project context like experimental tags or vehicle IDs, and the `ontology_tag`, informing the client of sensor data types (e.g., `Lidar`, `Camera`) for type-safe deserialization.

The `serialization_format` guides interpretation of the underlying serialization protocol used. Now the supported formats include:

- `Default`: The standard format.
- `Ragged`: Optimized for variable-length lists.
- `Image`: An optimized array format for high-resolution visual data.
- `ImageSequence`: Losslessly compressed images (PNG), each with its capture time: see [Image Sequences](#image-sequences).
- `ImageSequenceRaw`: Raw images, as a camera driver writes them (e.g. a ROS `sensor_msgs/Image`), each with its capture time: see [Image Sequences](#image-sequences).