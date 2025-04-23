# Test Suite

The `tests` folder contains the main rustvello test suite. This page contains information on the various components of the test suite, as well as guidelines for writing new tests.

## Unit Tests

The `tests` folder contains all unit tests for rustvello. These tests are intended to ensure all functionality works as intended.

### Running Unit Tests

To run the unit tests, navigate to the project root and run:

```bash
make test
```

This command will:

- Compile the Rust code.
- Run Rust unit tests.
- Run Python unit tests using `pytest`.

### Writing Unit Tests

When adding new functionality, you should also add matching unit tests.

- **Rust:** Place Rust unit tests within the module files, using the `#[cfg(test)]` module.
- **Python:** Place Python unit tests in the `tests` directory, following the existing structure.

## Doctests

Doctests are tests embedded in the documentation strings of your code.

### Running Doctests

To run doctests for Python, run:

```bash
make doctest
```

This will execute the examples in the docstrings and verify their output.

### Writing Doctests

Include examples in your docstrings under an `Examples` section. For example:

```python
def add(a, b):
    """
    Adds two numbers.

    Parameters
    ----------
    a : int
    b : int

    Returns
    -------
    int

    Examples
    --------
    >>> add(2, 3)
    5
    """
    return a + b
```

Ensure that your examples are correct and up-to-date.
