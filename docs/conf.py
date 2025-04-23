from __future__ import annotations

import datetime
import importlib.metadata

# Remove the sys.path modification if the package is installed
# sys.path.insert(0, os.path.abspath("../../py-rustvello/rustvello"))

# Use the package name as defined in your pyproject.toml
DISTRIBUTION_METADATA = importlib.metadata.metadata("rustvello")

# -- Project information -----------------------------------------------------
author = DISTRIBUTION_METADATA["Author"]
project = DISTRIBUTION_METADATA["Name"]
version = DISTRIBUTION_METADATA["Version"]
current_year = datetime.datetime.now().year
copyright = f"{current_year}, {author}"
release = version

# -- General configuration ---------------------------------------------------
extensions = [
    "autodoc2",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "myst_parser",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_inline_tabs",
]

# -- Autodoc settings ---------------------------------------------------
autodoc2_output_dir = "api/python"
autodoc2_render_plugin = "myst"
autodoc2_packages = [{"path": "../py-rustvello/rustvello"}]

# -- MyST settings ---------------------------------------------------
myst_enable_extensions = [
    "dollarmath",
    "amsmath",
    "deflist",
    "fieldlist",
    "html_admonition",
    "html_image",
    "colon_fence",
    "smartquotes",
    "replacements",
    "strikethrough",
    "substitution",
    "tasklist",
    "attrs_inline",
    "attrs_block",
]
myst_number_code_blocks = ["typescript"]
myst_heading_anchors = 2
myst_footnote_transition = True
myst_dmath_double_inline = True
myst_enable_checkboxes = True
myst_substitutions = {
    "role": "[role](#syntax/roles)",
    "directive": "[directive](#syntax/directives)",
}

# -- Options for HTML output -------------------------------------------------
html_theme = "furo"
html_title = "rustvello"
html_logo = "_static/logo.png"
html_favicon = "_static/logo.png"

language = "en"
html_static_path = ["_static"]
html_theme_options: dict[str, Any] = {
    "navigation_with_keys": True,
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/pynenc/rustvello",
            "html": """
                <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 16 16">
                    <path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z"></path>
                </svg>
            """,
            "class": "",
        },
    ],
    "source_repository": "https://github.com/pynenc/rustvello/",
    "source_branch": "main",
    "source_directory": "docs/",
    "announcement": f"<b>{version}</b> is now out! See the Changelog for details",
}
