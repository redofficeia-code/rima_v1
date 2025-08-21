import os
import pandas as pd
from typing import List

BASE_DIR = os.path.dirname(os.path.dirname(__file__))


def get_nv_headers_by_nums(nums: List[str]) -> list[dict]:
    nums = [n for n in (nums or []) if n]
    if not nums:
        return []

    try:
        raise RuntimeError("usar fallback CSV")
    except Exception:
        path = os.path.join(BASE_DIR, "data", "nv.csv")
        if not os.path.exists(path):
            return []
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        df = df[df["NUMNOTA"].isin(nums)]
        cols = [c for c in ["NUMNOTA", "NRUTCLIE", "FECHA", "SUCUR", "TOTAL", "ESTADO"] if c in df.columns]
        if not cols:
            return []
        out = df[cols].copy()
        return out.to_dict(orient="records")