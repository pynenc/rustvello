"""Protocol definitions for ElementKind."""

from __future__ import annotations

from typing import Optional

from rustvello.rustvello import PyElementKindRegistration


class ElementKindRegistration:
    """Registration details for an ElementKind."""

    _elem_kind_reg: PyElementKindRegistration

    def __init__(
        self, code: str, name: str, description: str, parent_code: Optional[str] = None
    ) -> None:
        """Initialize ElementKindRegistration."""
        self._elem_kind_reg = PyElementKindRegistration(
            code, name, description, parent_code
        )
