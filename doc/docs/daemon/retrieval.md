# Retrieval

Measurement data in Mosaico is accessed through the Flight `DoGet` endpoint for high-performance read operations. Unlike simple file downloads, this channel provides an interface for requesting precise data slices, dynamically assembled and streamed back as optimized Arrow batches.

## The Retrieval Protocol

Accessing data requires specifying the **Locator**, which defines the topic path, and an optional time range in nanoseconds.

The resolution process follows a coordinated sequence. Upon receiving a request, the server performs an index lookup in the metadata cache to identify physical data chunks intersecting the requested time window. This is followed by pruning, discarding chunks outside the query bounds to avoid redundant I/O. Once relevant segments are identified, the server streams the data by opening underlying files and delivering it in a high-throughput pipeline.

In the protocol, the `get_flight_info` call returns a list of resources, each containing an endpoint (the name of the topic or sequence, such as `my_sequence` or `my_sequence/my/topic`) and a ticket, an opaque binary blob used by the server in the `do_get` call to extract and stream the data. 

Calling `get_flight_info` on a sequence returns all topics associated with that sequence, whereas calling it on a specific topic returns only the endpoint and ticket for that topic.

```py title="Retrieval protocol in pseudo-code"
locator = "my_sequence/topic/1"
time_range = (start_ns, end_ns)  # optional

resources = get_flight_info(locator, time_range)
for res in resources:
    print(res.endpoint)
    data_stream = do_get(res.ticket)
```

## Metadata Context Headers

To provide full context, the data stream is prefixed with a Schema message containing embedded custom metadata. Mosaico injects context into this header for client reconstruction of the environment.

This includes *user metadata*, preserving original project context like experimental tags or vehicle IDs, and the *ontology tag*, informing the client of sensor data types (e.g., `Lidar`, `Camera`) for type-safe deserialization.

The *serialization format* guides interpretation of the underlying serialization protocol used. Now the supported formats include:

- `Default`: The standard Arrow columnar layout.
- `Ragged`: Optimized for variable-length lists.
- `Image`: An optimized array format for high-resolution visual data.