# CLI Reference

## Run 

Start the server locally with verbose logging:

```bash
mosaicod run [OPTIONS]
```

| Option | Default | Description |
| :--- | :--- | :--- |
| `--host` | `false` | Listen on all addresses, including LAN and public addresses. |
| `--port <PORT>` | `6726` | Port to listen on. |
| `--local-store <PATH>` | `None` | Enable storage of objects on the local filesystem at the specified directory path. |
