# {py:mod}`rustvello.muse.muse`

```{py:module} rustvello.muse.muse
```

```{autodoc2-docstring} rustvello.muse.muse
:allowtitles:
```

## Module Contents

### Classes

````{list-table}
:class: autosummary longtable
:align: left

* - {py:obj}`Muse <rustvello.muse.muse.Muse>`
  - ```{autodoc2-docstring} rustvello.muse.muse.Muse
    :summary:
    ```
````

### API

`````{py:class} Muse(config: rustvello.config.Config)
:canonical: rustvello.muse.muse.Muse

```{autodoc2-docstring} rustvello.muse.muse.Muse
```

```{rubric} Initialization
```

```{autodoc2-docstring} rustvello.muse.muse.Muse.__init__
```

````{py:attribute} _muse
:canonical: rustvello.muse.muse.Muse._muse
:type: rustvello.rustvello.PyMuse
:value: >
   None

```{autodoc2-docstring} rustvello.muse.muse.Muse._muse
```

````

````{py:method} initialize(timeout: float | None = None) -> None
:canonical: rustvello.muse.muse.Muse.initialize
:async:

```{autodoc2-docstring} rustvello.muse.muse.Muse.initialize
```

````

````{py:method} create(config: rustvello.config.Config, timeout: float | None = None) -> rustvello.muse.muse.Muse
:canonical: rustvello.muse.muse.Muse.create
:async:
:classmethod:

```{autodoc2-docstring} rustvello.muse.muse.Muse.create
```

````

````{py:method} is_initialized() -> bool
:canonical: rustvello.muse.muse.Muse.is_initialized

```{autodoc2-docstring} rustvello.muse.muse.Muse.is_initialized
```

````

````{py:method} register_element(kind_code: str, name: str, metadata: dict[str, str], parent_id: int | None = None) -> int
:canonical: rustvello.muse.muse.Muse.register_element
:async:

```{autodoc2-docstring} rustvello.muse.muse.Muse.register_element
```

````

````{py:method} send_metric(local_elem_id: int, metric_code: str, value: float) -> None
:canonical: rustvello.muse.muse.Muse.send_metric
:async:

```{autodoc2-docstring} rustvello.muse.muse.Muse.send_metric
```

````

````{py:method} replay(replay_path: str) -> None
:canonical: rustvello.muse.muse.Muse.replay
:async:

```{autodoc2-docstring} rustvello.muse.muse.Muse.replay
```

````

`````
