# py-rustvello

Python bindings for [rustvello](https://github.com/pynenc/rustvello), the Rust core of [pynenc](https://github.com/pynenc/pynenc).

## Overview

This package provides PyO3-based Python bindings that expose rustvello's task engine to Python. It is primarily consumed by the [pynenc](https://github.com/pynenc/pynenc) framework.

## Building from Source

### Prerequisites

- Python 3.11+
- [maturin](https://www.maturin.rs/)
- Rust toolchain (1.78+)

### Development Build

```bash
# From the repository root:
make develop

# Or directly:
maturin develop --release -m py-rustvello/Cargo.toml
```

### Wheel Build

```bash
maturin build --release -m py-rustvello/Cargo.toml --out dist
```

## Usage

Most users should use [pynenc](https://docs.pynenc.org) instead of importing `rustvello` directly:

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    return x + y
```

For direct access to the bindings:

```python
import rustvello
```
