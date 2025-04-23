# {py:mod}`rustvello.config.config`

```{py:module} rustvello.config.config
```

```{autodoc2-docstring} rustvello.config.config
:allowtitles:
```

## Module Contents

### Classes

````{list-table}
:class: autosummary longtable
:align: left

* - {py:obj}`Config <rustvello.config.config.Config>`
  - ```{autodoc2-docstring} rustvello.config.config.Config
    :summary:
    ```
````

### API

`````{py:class} Config(endpoints: list[str], client_type: rustvello.rustvello.ClientType, default_resolution: rustvello.rustvello.TimestampResolution, element_kinds: list[rustvello.proto.ElementKindRegistration], metric_definitions: list[rustvello.proto.MetricDefinition], max_reg_elem_retries: int, max_endpoint_retries: int | None, recording_enabled: bool, recording_path: str | None = None)
:canonical: rustvello.config.config.Config

```{autodoc2-docstring} rustvello.config.config.Config
```

```{rubric} Initialization
```

```{autodoc2-docstring} rustvello.config.config.Config.__init__
```

````{py:attribute} _config
:canonical: rustvello.config.config.Config._config
:type: rustvello.rustvello.PyConfig
:value: >
   None

```{autodoc2-docstring} rustvello.config.config.Config._config
```

````

`````
