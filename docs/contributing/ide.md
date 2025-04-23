# IDE Configuration

Using an integrated development environment (IDE) and configuring it properly will help you work on rustvello more effectively. This page contains some recommendations for configuring popular IDEs.

## Visual Studio Code

Make sure to configure VSCode to use the virtual environment created by the Makefile.

### Extensions

The extensions below are recommended.

#### Rust Analyzer

If you work on the Rust code, you will need the [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer) extension. This extension provides code completion for Rust code.

For it to work well for the rustvello codebase, add the following settings to your `.vscode/settings.json`:

```json
{
  "rust-analyzer.cargo.features": "all",
  "rust-analyzer.cargo.targetDir": true
}
```

#### Ruff

The [Ruff](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff) extension will help you conform to the formatting requirements of the Python code. Configure the extension to use the Ruff installed in your environment:

```json
{
  "ruff.importStrategy": "fromEnvironment"
}
```

### Settings

Here are some recommended settings for your `.vscode/settings.json`:

```json
{
  "python.pythonPath": ".venv/bin/python",
  "python.testing.pytestEnabled": true,
  "editor.formatOnSave": true,
  "[python]": {
    "editor.defaultFormatter": "charliermarsh.ruff",
    "editor.codeActionsOnSave": {
      "source.fixAll": true
    }
  },
  "ruff.importStrategy": "fromEnvironment",
  "terminal.integrated.defaultProfile.osx": "bash",
  "terminal.integrated.profiles.osx": {
    "bash": {
      "path": "bash",
      "args": [
        "-c",
        "if command -v poetry >/dev/null 2>&1; then poetry shell; else exec bash; fi"
      ]
    }
  }
}
```
