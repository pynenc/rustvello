# Replaying Events

rustvello allows you to replay previously recorded events from a file. This is useful for testing, simulating system behavior, or reprocessing historical data.

## Using the Replayer

To replay events, use the `replay` method of the Muse client.

````{tab-set-code}

```{code-block} python
await muse.replay("events.bin")
```

```{code-block} rust
use std::path::Path;

muse.replay(Path::new("events.bin")).await?;
```
````

**Note:** Ensure that the serialization format of the recording file matches the expected format (based on the file extension).

## Command-Line Interface

You can also use the command-line interface (CLI) tool to replay events.

### Replaying with the CLI

```shell
rustvello-cli replay --input events.bin --poet-url http://localhost:8000
```

#### CLI Options

- `--input`: Path to the recording file.
- `--poet-url`: URL of the Muse system endpoint (defaults to `http://localhost:8000`).

**Example:**

```shell
rustvello-cli replay --input events.json --poet-url http://localhost:8000
```

This command will replay the events from `events.json` to the specified Muse system.

## Handling Serialization Formats

The CLI tool automatically determines the serialization format based on the file extension:

- `.bin` for Bincode serialization
- `.json` for JSON serialization

Ensure your recording files have the correct extension to avoid errors during replay.
