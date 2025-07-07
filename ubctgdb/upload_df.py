from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from .upload_csv import upload_csv
from .core import NULL_TOKEN

def upload_dataframe(
    df: pd.DataFrame,
    *,
    table: str,
    schema: str | None = None,
    host: str | None = None,
    port: int | None = None,
    replace_table: bool = False,
    csv_kwargs: dict[str, Any] | None = None,
    **load_csv_kw,
) -> None:

    csv_kwargs = csv_kwargs or {}
    with tempfile.NamedTemporaryFile(
        suffix=".csv", prefix="df_", delete=False
    ) as tmp:
        path = Path(tmp.name)
    try:
        df.to_csv(
            path, index=False,
            na_rep=NULL_TOKEN,
            **csv_kwargs,
        )
        upload_csv(
            csv_path=path,
            table=table,
            schema=schema,
            host=host,
            port=port,
            header=True,
            replace_table=replace_table,
            **load_csv_kw,
        )
    finally:
        path.unlink(missing_ok=True)