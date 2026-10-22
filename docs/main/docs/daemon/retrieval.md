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

## Sequence List

To list every sequence available in the system, call `list_flights` with `limit: 0`:

```py title="list_sequences"
# List all sequences in the system, oldest first
cmd = ListFlightsCmd(limit=0)
flight_info = list_flights(cmd.SerializeToString())
```

Each returned `FlightInfo` carries the sequence name in its descriptor path, which can then be used to retrieve specific topics or data slices.

### Pagination

Without `limit: 0`, `list_flights` returns one page at a time. Its `Criteria.expression` carries a protobuf `ListFlightsCmd`:

```proto
message ListFlightsCmd {
  string locator = 1;                   // "" or "/": the root, i.e. list sequences
  optional SequenceOrder order_by = 2;  // absent: the cursor's order, else SEQUENCE_ORDER_CREATED_AT
  optional uint32 limit = 3;            // absent: 50; 0: no limit
  SequenceCursor cursor = 4;            // absent: first page
}
```

An empty expression is a command with every field at its default: the first 50 sequences, oldest first. Ordering is always ascending.

Each returned `FlightInfo` also carries a `SequenceListItemAppMetadata` in `app_metadata`. Its `cursor` points just past that sequence: copy it into the next request to continue. The cursor also carries its order, so `order_by` can be left out when sending it back.

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
- **Errors.** `InvalidArgument` is returned when:
  - the expression is not a valid `ListFlightsCmd`;
  - the locator is not the root;
  - `order_by` is set and the cursor was built for a different order.

The Python SDK wraps this in `MosaicoClient.list_sequences_page()` and `MosaicoClient.iter_sequences()`. `MosaicoClient.list_sequences()` returns every sequence at once.

## Metadata Context Headers

To provide full context, the data stream is prefixed with a schema message containing embedded custom metadata. Mosaico injects context into this header for client reconstruction of the environment.

This includes `user_metadata`, preserving original project context like experimental tags or vehicle IDs, and the `ontology_tag`, informing the client of sensor data types (e.g., `Lidar`, `Camera`) for type-safe deserialization.

The `serialization_format` guides interpretation of the underlying serialization protocol used. Now the supported formats include:

- `Default`: The standard format.
- `Ragged`: Optimized for variable-length lists.
- `Image`: An optimized array format for high-resolution visual data.