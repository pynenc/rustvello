# IDE Configuration

Recommended IDE setup for working on rustvello.

## Visual Studio Code

### Extensions

- **[rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer)**: Rust language support with code completion, inline errors, and refactoring.
- **[Ruff](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)**: Python linting and formatting.

### Settings

Add the following to `.vscode/settings.json`:

```json
{
  "rust-analyzer.cargo.features": "all",
  "rust-analyzer.cargo.targetDir": true,
  "python.pythonPath": ".venv/bin/python",
  "python.testing.pytestEnabled": true,
  "editor.formatOnSave": true,
  "[python]": {
    "editor.defaultFormatter": "charliermarsh.ruff",
    "editor.codeActionsOnSave": {
      "source.fixAll": true
    }
  },
  "ruff.importStrategy": "fromEnvironment"
}
```

### Tips

- Set `"rust-analyzer.cargo.features": "all"` so that rust-analyzer checks all feature-gated code (mem, sqlite, etc.).
- Set `"rust-analyzer.cargo.targetDir": true` to avoid build conflicts between rust-analyzer and `cargo build`.
- If working on the Python bindings (`py-rustvello`), run `make develop` first to build the native extension.
