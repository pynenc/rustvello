# Installation

rustvello is available for both Rust and Python. Installation is straightforward using the respective package managers.

## Installing rustvello

Install Python

```bash
pip install rustvello
```

Install Rust

```shell
cargo add rustvello

# Or add it directly to your Cargo.toml
[dependencies]
rustvello = "0.1.0"
```

## Importing

To use the library, import it into your project:

````{tab-set-code}
```{code-block} python
from rustvello import Muse, Config
```

```{code-block} rust
use rustvello::prelude::*;
```
````

## Feature Flags (Rust Only)

Depending on your use case, you might want to enable optional features to extend the functionality of rustvello in Rust. These are made optional to minimize the footprint.

### Optional Features

- `recording`: Enables event recording and replaying functionality.
- `cli`: Includes the command-line interface utilities.
- `mock-client`: Provides a mock client implementation for testing.
- `poet-client`: Enables the Poet client for communication with the Muse system.

To enable these features, specify them in your `Cargo.toml`:

```toml
[dependencies]
rustvello = { version = "0.1.0", features = ["recording", "poet-client"] }
```

---

**Note:** Replace `"0.1.0"` with the actual version of rustvello you're using.
