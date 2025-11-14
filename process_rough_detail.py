"""Rough detail processing pipeline.

This script reads a collection of Excel workbooks, reproduces the
calculations normally performed in Excel for the "Rough Detail" report,
and writes the fully calculated table (values only) to
``Rough_Detail_Calculated.xlsx``.

The logic follows the specification provided by the user and relies on
vectorised pandas operations.  Intermediate helper tables are computed
for clarity and, when available, written alongside the final output.
"""

from __future__ import annotations

import os
import re
from typing import Dict, Iterable, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _normalise_string(series: pd.Series) -> pd.Series:
    """Return an upper-cased, trimmed string series (NaNs -> empty string)."""

    return series.fillna("").astype(str).str.strip().str.upper()


def _strip_string(series: pd.Series) -> pd.Series:
    """Return a trimmed string series preserving original casing."""

    return series.fillna("").astype(str).str.strip()


def _extract_numeric_prefix(value: str) -> Optional[int]:
    """Extract the leading integer portion of a lot number string."""

    if not isinstance(value, str):
        value = "" if pd.isna(value) else str(value)
    match = re.match(r"^(\d+)", value.strip())
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _safe_mean(series: pd.Series) -> float:
    """Return the mean ignoring NaT/NaN, keeping NaN for empty groups."""

    if series.empty:
        return np.nan
    return series.mean()


def _find_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    """Locate a column in ``df`` that matches any candidate (case insensitive)."""

    lower_map = {col.lower(): col for col in df.columns}
    lowered_candidates = [cand.lower() for cand in candidates]

    for cand in lowered_candidates:
        if cand in lower_map:
            return lower_map[cand]

    for cand in lowered_candidates:
        for lower_name, original in lower_map.items():
            if cand in lower_name:
                return original
    return None


def _detect_lab_rate_columns(df: pd.DataFrame, lab_keyword: str) -> Tuple[Optional[str], Optional[str]]:
    """Detect possible per-piece and per-carat expense columns for a lab."""

    lab_keyword = lab_keyword.lower()
    per_piece = None
    per_cts = None
    for column in df.columns:
        c = column.lower()
        if lab_keyword in c and "exp" in c:
            if "ct" in c:
                per_cts = column
            elif "pcs" in c or "piece" in c:
                per_piece = column
            elif per_piece is None:
                per_piece = column
    return per_piece, per_cts


def _load_excel(path: str, **kwargs) -> pd.DataFrame:
    """Read an Excel file, returning an empty frame if missing."""

    if not os.path.exists(path):
        print(f"Warning: file '{path}' not found. Using empty dataframe.")
        return pd.DataFrame()
    return pd.read_excel(path, **kwargs)


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------


def load_lot_list(path: str) -> pd.DataFrame:
    df = _load_excel(path)
    if df.empty:
        return df
    lot_col = _find_column(df, ["LotNo", "Lot No", "LOTNO"])
    code_col = _find_column(df, ["CODE", "Code"])
    if lot_col is None or code_col is None:
        raise KeyError("Lot list must contain LotNo and CODE columns.")
    df = df.copy()
    df["LotNo_key"] = _normalise_string(df[lot_col])
    df.rename(columns={lot_col: "LotNo", code_col: "CODE"}, inplace=True)
    return df


def load_multi_box(path: str) -> pd.DataFrame:
    df = _load_excel(path)
    if df.empty:
        return df
    lot_col = _find_column(df, ["Lot", "LotNo", "Lot No"])
    name_col = _find_column(df, ["Multi Box Name", "Multi Box", "MultiBoxName"])
    if lot_col is None or name_col is None:
        raise KeyError("Multi Box list must contain Lot and Multi Box Name columns.")
    df = df.copy()
    df["LotNo_key"] = _normalise_string(df[lot_col])
    df.rename(columns={name_col: "Multi Box Name"}, inplace=True)
    return df[["LotNo_key", "Multi Box Name"]]


def load_sales_data(path: str) -> pd.DataFrame:
    df = _load_excel(path)
    if df.empty:
        return df

    stone_col = _find_column(df, ["Stone Id", "STONE_ID", "StoneID", "STONE ID"])
    party_amt_col = _find_column(df, ["Party Amt", "Party Amount", "Party_Amt"])
    lab_col = _find_column(df, ["Lab", "LAB", "Lab Name"])
    cts_col = _find_column(df, ["cts", "Cts", "Carat", "Carats"])

    if stone_col is None or party_amt_col is None or lab_col is None or cts_col is None:
        raise KeyError("Sales data must contain Stone Id, Party Amt, Lab, and cts columns.")

    df = df.copy()

    gia_cols = _detect_lab_rate_columns(df, "GIA")
    hrd_cols = _detect_lab_rate_columns(df, "HRD")
    igi_cols = _detect_lab_rate_columns(df, "IGI")

    expenses = []
    for _, row in df.iterrows():
        lab_value = str(row[lab_col]).strip().upper()
        cts_value = float(row[cts_col]) if not pd.isna(row[cts_col]) else 0.0
        expense = 0.0

        if lab_value in {"", "NC", "NCSS", "KGLAB", "NA"}:
            expenses.append(0.0)
            continue

        if lab_value.startswith("GIA") and (gia_cols[0] or gia_cols[1]):
            per_piece_col, per_cts_col = gia_cols
        elif lab_value.startswith("HRD") and (hrd_cols[0] or hrd_cols[1]):
            per_piece_col, per_cts_col = hrd_cols
        elif lab_value.startswith("IGI") and (igi_cols[0] or igi_cols[1]):
            per_piece_col, per_cts_col = igi_cols
        else:
            expenses.append(0.0)
            continue

        if per_piece_col and not pd.isna(row.get(per_piece_col)):
            expense += float(row[per_piece_col])
        if per_cts_col and not pd.isna(row.get(per_cts_col)):
            expense += float(row[per_cts_col]) * cts_value

        expenses.append(expense)

    df["Lab Expense"] = expenses
    df["Sales Value"] = df[party_amt_col].fillna(0.0) - df["Lab Expense"]
    df["STONE_ID_norm"] = _normalise_string(df[stone_col])
    return df


def load_or_uncut(path: str) -> pd.DataFrame:
    df = _load_excel(path)
    if df.empty:
        return df
    lot_col = _find_column(df, ["Lot", "LotNo", "Lot No"])
    pcs_col = _find_column(df, ["Pcs", "PCS"])
    cts_col = _find_column(df, ["Cts", "CTS"])
    amount_col = _find_column(df, ["Amount", "Value", "AMOUNT"])
    if lot_col is None:
        raise KeyError("OR_UNCUT requires a Lot column.")
    df = df.copy()
    df["LotNo_key"] = _normalise_string(df[lot_col])
    rename_map = {}
    if pcs_col:
        df[pcs_col] = pd.to_numeric(df[pcs_col], errors="coerce").fillna(0.0)
        rename_map[pcs_col] = "OR UNCUT PCS"
    if cts_col:
        df[cts_col] = pd.to_numeric(df[cts_col], errors="coerce").fillna(0.0)
        rename_map[cts_col] = "OR UNCUT CTS"
    if amount_col:
        df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce").fillna(0.0)
        rename_map[amount_col] = "OR UNCUT VALUE"
    df.rename(columns=rename_map, inplace=True)
    return df


def load_direct_sales(path: str) -> pd.DataFrame:
    df = _load_excel(path)
    if df.empty:
        return df
    lot_col = _find_column(df, ["Lot No", "LotNo", "Lot"])
    pcs_col = _find_column(df, ["OR UNCUT PCS", "PCS"])
    cts_col = _find_column(df, ["OR UNCUT CTS", "CTS"])
    value_col = _find_column(df, ["OR UNCUT VALUE", "Amount", "Value"])
    if lot_col is None:
        raise KeyError("Direct sales requires a lot column.")
    df = df.copy()
    df["LotNo_key"] = _normalise_string(df[lot_col])
    rename_map = {}
    if pcs_col:
        df[pcs_col] = pd.to_numeric(df[pcs_col], errors="coerce").fillna(0.0)
        rename_map[pcs_col] = "DIRECT SALES PCs"
    if cts_col:
        df[cts_col] = pd.to_numeric(df[cts_col], errors="coerce").fillna(0.0)
        rename_map[cts_col] = "DIRECT SALES CTS"
    if value_col:
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce").fillna(0.0)
        rename_map[value_col] = "DIRECT SALES VALUE"
    df.rename(columns=rename_map, inplace=True)
    return df


def load_damage(path: str) -> pd.DataFrame:
    df = _load_excel(path)
    if df.empty:
        return df
    lot_col = _find_column(df, ["LOTNO", "LotNo", "Lot"])
    if lot_col is None:
        raise KeyError("Damage detail requires a LOTNO column.")
    df = df.copy()
    df["LotNo_key"] = _normalise_string(df[lot_col])
    result_cols = {}
    for name in ["BREAKAGE", "WORKER MISTAKE", "Grand Total", "COMPROMISE"]:
        col = _find_column(df, [name])
        if col:
            df[name.upper()] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            df[name.upper()] = 0.0
        result_cols[name.upper()] = df[name.upper()]
    return df[["LotNo_key", "BREAKAGE", "WORKER MISTAKE", "GRAND TOTAL", "COMPROMISE"]]


def load_first_signer(path: str) -> pd.DataFrame:
    df = _load_excel(path)
    if df.empty:
        return df
    lot_col = _find_column(df, ["Lotno", "Lot No", "Lot"])
    if lot_col is None:
        raise KeyError("1st Signer Missing data requires a Lotno column.")
    df = df.copy()
    df["LotNo_key"] = _normalise_string(df[lot_col])
    amount_col = _find_column(df, ["Amount", "Value", "Adjustment"])
    if amount_col is None:
        if df.shape[1] >= 6:
            amount_col = df.columns[5]
        else:
            amount_col = df.columns[-1]
    df["1st Result Uncut Adjustment"] = pd.to_numeric(df[amount_col], errors="coerce").fillna(0.0)
    return df[["LotNo_key", "1st Result Uncut Adjustment"]]


def load_nns_data(path: str) -> pd.DataFrame:
    df = _load_excel(path)
    if df.empty:
        return df
    df = df.copy()
    disp_col = _find_column(df, ["DISP_LOTNO"])
    main_pkt_col = _find_column(df, ["MAIN_PKTNO"])
    tag_col = _find_column(df, ["TAG_NAME"])
    stone_status_col = _find_column(df, ["STONE_STATUS"])
    sidtype_col = _find_column(df, ["SIDTYPE"])
    yns_status_col = _find_column(df, ["YNS_STATUS"])
    shp_col = _find_column(df, ["SHP"])
    avg_col = _find_column(df, ["AVG_MPM_PD"])
    mpm_col = _find_column(df, ["MPM_PD"])
    max_col = _find_column(df, ["MAX_CUR_FINAL_RATE_PD"])
    rate_col = _find_column(df, ["RATE_TYPE"])
    stone_id_col = _find_column(df, ["STONE_ID", "Stone Id", "STONE ID"])

    required = [disp_col, main_pkt_col, tag_col, stone_status_col, sidtype_col, yns_status_col, shp_col, avg_col, mpm_col, max_col, rate_col]
    if any(col is None for col in required):
        raise KeyError("NNS data missing required columns.")

    df.rename(
        columns={
            disp_col: "DISP_LOTNO",
            main_pkt_col: "MAIN_PKTNO",
            tag_col: "TAG_NAME",
            stone_status_col: "STONE_STATUS",
            sidtype_col: "SIDTYPE",
            yns_status_col: "YNS_STATUS",
            shp_col: "SHP",
            avg_col: "AVG_MPM_PD",
            mpm_col: "MPM_PD",
            max_col: "MAX_CUR_FINAL_RATE_PD",
            rate_col: "RATE_TYPE",
        },
        inplace=True,
    )

    df["DISP_LOTNO_key"] = _normalise_string(df["DISP_LOTNO"])
    df["MAIN_PKTNO_key"] = _strip_string(df["MAIN_PKTNO"])
    df["TAG_NAME_key"] = _strip_string(df["TAG_NAME"])
    if stone_id_col:
        df["STONE_ID_norm"] = _normalise_string(df[stone_id_col])
    else:
        df["STONE_ID_norm"] = ""

    for column in ["AVG_MPM_PD", "MPM_PD", "MAX_CUR_FINAL_RATE_PD"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)

    return df


def load_current_getrate(path: str) -> pd.DataFrame:
    df = _load_excel(path)
    if df.empty:
        return df
    df = df.copy()
    df.rename(
        columns={
            _find_column(df, ["Lotno", "Lot No", "Lot"]): "Lotno",
            _find_column(df, ["Main Pktno", "MainPktno", "MAIN PKTNO"]): "Main Pktno",
            _find_column(df, ["Pktno", "Pkt No"]): "Pktno",
            _find_column(df, ["Tag", "TAG"]): "Tag",
            _find_column(df, ["Shape", "SHP"]): "Shape",
            _find_column(df, ["Amount", "Value", "AMOUNT"]): "Amount",
            _find_column(df, ["AF Curr PD Amt", "AF CURR PD AMT", "AF Current PD Amt"]): "AF Curr PD Amt",
        },
        inplace=True,
    )
    df["LotNo_key"] = _normalise_string(df["Lotno"])
    df["Main_Pktno_key"] = _strip_string(df["Main Pktno"])
    df["Tag_key"] = _strip_string(df["Tag"])
    for column in ["Amount", "AF Curr PD Amt"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    return df


def load_result_getrate(path: str) -> pd.DataFrame:
    df = _load_excel(path)
    if df.empty:
        return df
    df = df.copy()
    df.rename(
        columns={
            _find_column(df, ["Lotno", "Lot No", "Lot"]): "Lotno",
            _find_column(df, ["main Pktno", "Main Pktno", "MainPktno"]): "main Pktno",
            _find_column(df, ["Tag", "TAG"]): "Tag",
            _find_column(df, ["Amount", "Value", "AMOUNT"]): "Amount",
            _find_column(df, ["AF Curr PD Amt", "AF CURR PD AMT"]): "AF Curr PD Amt",
        },
        inplace=True,
    )
    df["LotNo_key"] = _normalise_string(df["Lotno"])
    df["main_Pktno_key"] = _strip_string(df["main Pktno"])
    df["Tag_key"] = _strip_string(df["Tag"])
    for column in ["Amount", "AF Curr PD Amt"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    return df


def load_lot_check(possible_paths: Iterable[str]) -> pd.DataFrame:
    for path in possible_paths:
        if os.path.exists(path):
            df = pd.read_excel(path)
            lot_col = _find_column(df, ["Lot Not to Consider", "Lot", "LotNo"])
            if lot_col is None:
                continue
            df["LotNo_key"] = _normalise_string(df[lot_col])
            return df[["LotNo_key"]]
    return pd.DataFrame(columns=["LotNo_key"])


# ---------------------------------------------------------------------------
# Enrichment logic
# ---------------------------------------------------------------------------


def enrich_sales_mapping(sales_df: pd.DataFrame) -> pd.DataFrame:
    if sales_df.empty:
        return pd.DataFrame(columns=["STONE_ID_norm", "Sales Value", "Lab Expense"])
    return sales_df[["STONE_ID_norm", "Sales Value", "Lab Expense"]]


def enrich_nns_data(
    nns: pd.DataFrame,
    sales_map: pd.DataFrame,
    current_remd_lookup: Optional[Dict[Tuple[str, str], bool]] = None,
) -> pd.DataFrame:
    if nns.empty:
        return nns

    nns = nns.copy()

    nns["Key"] = (
        nns["DISP_LOTNO_key"].fillna("")
        + "#"
        + nns["MAIN_PKTNO_key"].fillna("")
        + "#"
        + nns["TAG_NAME_key"].fillna("")
    )

    sum_max = nns.groupby("DISP_LOTNO_key")["MAX_CUR_FINAL_RATE_PD"].sum(min_count=1)
    sum_avg = nns.groupby("DISP_LOTNO_key")["AVG_MPM_PD"].sum(min_count=1)
    price_change = (sum_max / sum_avg).replace([np.inf, -np.inf], np.nan) - 1
    price_change = price_change.fillna(0.0)
    nns["PRICE CHANGE"] = nns["DISP_LOTNO_key"].map(price_change).fillna(0.0)

    rate_type_clean = _strip_string(nns["RATE_TYPE"]).str.lower()
    new_current = nns["MAX_CUR_FINAL_RATE_PD"] * (1 + nns["PRICE CHANGE"])
    nns["NEW CURRENT"] = np.where(rate_type_clean == "avgreceived", new_current, nns["MAX_CUR_FINAL_RATE_PD"])

    nns["COUNT"] = nns.groupby("Key")["Key"].transform("size")

    sales_lookup = {}
    if not sales_map.empty:
        sales_lookup = dict(zip(sales_map["STONE_ID_norm"], sales_map["Sales Value"]))

    sidtype_clean = _strip_string(nns["SIDTYPE"]).str.lower()
    stone_id = nns["STONE_ID_norm"].fillna("")

    sold_values = []
    for stone, sidtype, avg_value in zip(stone_id, sidtype_clean, nns["AVG_MPM_PD"]):
        if stone == "" and sidtype == "labreceivedfinal":
            sold_values.append(avg_value)
        else:
            sold_values.append(float(sales_lookup.get(stone, 0.0)))

    nns["Sold"] = sold_values
    nns["Sales+Current"] = np.where(np.isclose(nns["Sold"], 0.0), nns["NEW CURRENT"], nns["Sold"])

    nns["AVG_MPM_PD+By Product"] = np.where(
        np.isclose(nns["AVG_MPM_PD"], 0.0), nns["MPM_PD"], nns["AVG_MPM_PD"]
    )

    lot_totals = nns.groupby("DISP_LOTNO_key")["AVG_MPM_PD+By Product"].transform("sum")
    nns["Running %"] = np.where(
        np.isclose(lot_totals, 0.0), 0.0, nns["AVG_MPM_PD+By Product"] / lot_totals
    )

    nns["_SHP_CLEAN"] = _strip_string(nns["SHP"]).str.upper()
    nns["R-EMD PCS"] = nns.groupby(["DISP_LOTNO_key", "MAIN_PKTNO_key"])["_SHP_CLEAN"].transform(
        lambda s: (s == "R-EMD").any()
    )

    current_lookup = current_remd_lookup or {}
    pairs = list(zip(nns["DISP_LOTNO_key"], nns["MAIN_PKTNO_key"]))
    current_flags = np.array([bool(current_lookup.get(pair, False)) for pair in pairs])

    nns["R-EMD LOSS PCS"] = nns["R-EMD PCS"] & ~current_flags
    nns["R-EMD GAIN PCS"] = ~nns["R-EMD PCS"] & current_flags

    return nns.drop(columns=["_SHP_CLEAN"], errors="ignore")


def enrich_current_getrate(
    current: pd.DataFrame,
    nns_remd_lookup: Dict[Tuple[str, str], bool],
) -> Tuple[pd.DataFrame, Dict[Tuple[str, str], bool]]:
    if current.empty:
        return current, {}

    current = current.copy()
    current["Lotno2"] = current["Lotno"].apply(lambda x: _extract_numeric_prefix(str(x) if not pd.isna(x) else ""))
    current["P KEY"] = (
        _strip_string(current["Lotno"]).fillna("")
        + "#"
        + current["Main_Pktno_key"].fillna("")
        + "#"
        + current["Tag_key"].fillna("")
    )
    current["KEY"] = (
        current["Lotno2"].fillna("").astype(str)
        + "#"
        + current["Main_Pktno_key"].fillna("")
    )
    current["DUPLICATE"] = current.groupby("P KEY")["P KEY"].transform("size")

    current["_SHAPE_CLEAN"] = _strip_string(current["Shape"]).str.upper()
    group_any = current.groupby(["LotNo_key", "Main_Pktno_key"])["_SHAPE_CLEAN"].transform(lambda s: (s == "R-EMD").any())
    current["R-EMD PCS"] = group_any

    pairs = list(zip(current["LotNo_key"], current["Main_Pktno_key"]))
    nns_flags = np.array([bool(nns_remd_lookup.get(pair, False)) for pair in pairs])

    current["R-EMD LOSS PCS"] = current["R-EMD PCS"] & ~nns_flags
    current["R-EMD GAIN PCS"] = ~current["R-EMD PCS"] & nns_flags

    current_lookup = {}
    for (lot, main), flag in zip(pairs, current.groupby(["LotNo_key", "Main_Pktno_key"])["R-EMD PCS"].transform("any")):
        current_lookup[(lot, main)] = bool(flag)

    return current.drop(columns=["_SHAPE_CLEAN"], errors="ignore"), current_lookup


def enrich_result_getrate(result: pd.DataFrame) -> pd.DataFrame:
    if result.empty:
        return result
    result = result.copy()
    result["Lotno2"] = result["Lotno"].apply(lambda x: _extract_numeric_prefix(str(x) if not pd.isna(x) else ""))
    result["Key"] = (
        result["Lotno2"].fillna("").astype(str)
        + "#"
        + result["main_Pktno_key"].fillna("")
    )
    result["P KEY"] = (
        result["Lotno2"].fillna("").astype(str)
        + "#"
        + result["main_Pktno_key"].fillna("")
        + "#"
        + result["Tag_key"].fillna("")
    )
    result["Lotno3"] = (
        _strip_string(result["Lotno"]).fillna("")
        + "#"
        + result["main_Pktno_key"].fillna("")
        + "#"
        + result["Tag_key"].fillna("")
    )
    return result


# ---------------------------------------------------------------------------
# Aggregations used by Rough Detail
# ---------------------------------------------------------------------------


def aggregate_or_uncut(or_uncut: pd.DataFrame) -> pd.DataFrame:
    if or_uncut.empty:
        return pd.DataFrame(columns=["LotNo_key", "OR UNCUT PCS", "OR UNCUT CTS", "OR UNCUT VALUE"])
    agg = (
        or_uncut.groupby("LotNo_key")[[col for col in ["OR UNCUT PCS", "OR UNCUT CTS", "OR UNCUT VALUE"] if col in or_uncut.columns]]
        .sum()
        .reset_index()
    )
    for col in ["OR UNCUT PCS", "OR UNCUT CTS", "OR UNCUT VALUE"]:
        if col not in agg.columns:
            agg[col] = 0.0
    return agg


def aggregate_direct_sales(direct: pd.DataFrame) -> pd.DataFrame:
    if direct.empty:
        return pd.DataFrame(columns=["LotNo_key", "DIRECT SALES PCs", "DIRECT SALES CTS", "DIRECT SALES VALUE"])
    agg = (
        direct.groupby("LotNo_key")[[col for col in ["DIRECT SALES PCs", "DIRECT SALES CTS", "DIRECT SALES VALUE"] if col in direct.columns]]
        .sum()
        .reset_index()
    )
    for col in ["DIRECT SALES PCs", "DIRECT SALES CTS", "DIRECT SALES VALUE"]:
        if col not in agg.columns:
            agg[col] = 0.0
    return agg


def aggregate_damage(damage: pd.DataFrame) -> pd.DataFrame:
    if damage.empty:
        return pd.DataFrame(columns=["LotNo_key", "BREAKAGE", "WORKER MISTAKE", "GRAND TOTAL", "COMPROMISE"])
    agg = (
        damage.groupby("LotNo_key")[["BREAKAGE", "WORKER MISTAKE", "GRAND TOTAL", "COMPROMISE"]]
        .sum()
        .reset_index()
    )
    return agg


def aggregate_first_signer(first_signer: pd.DataFrame) -> pd.DataFrame:
    if first_signer.empty:
        return pd.DataFrame(columns=["LotNo_key", "1st Result Uncut Adjustment"])
    agg = first_signer.groupby("LotNo_key")["1st Result Uncut Adjustment"].sum().reset_index()
    return agg


def aggregate_nns_for_lot(nns: pd.DataFrame) -> Dict[str, pd.Series]:
    if nns.empty:
        keys = [
            "by_product_mpm",
            "total_income",
            "total_current_income",
            "total_sales_current",
            "sold_value",
            "comp_value",
            "stone_bank_value",
            "lab_received_value",
            "r_emd_value",
            "r_emd_loss_amount",
            "r_emd_gain_amount",
        ]
        return {key: pd.Series(dtype=float) for key in keys}

    lot = nns["DISP_LOTNO_key"]
    stone_status = _strip_string(nns["STONE_STATUS"]).str.lower()
    sidtype = _strip_string(nns["SIDTYPE"]).str.lower()
    yns_status = _strip_string(nns["YNS_STATUS"]).str.lower()
    shp = _strip_string(nns["SHP"]).str.upper()

    mask_by_product = stone_status == "by products"
    mask_non_by = ~mask_by_product
    mask_not_inprocess = mask_non_by & (sidtype != "inprocess")
    mask_stone_bank = mask_non_by & (yns_status == "stone bank")
    mask_lab_received = mask_non_by & (sidtype == "labreceivedfinal")
    mask_r_emd = mask_non_by & (shp == "R-EMD")

    aggregates = {
        "by_product_mpm": nns.loc[mask_by_product].groupby(lot)["MPM_PD"].sum(min_count=1),
        "total_income": nns.loc[mask_non_by].groupby(lot)["AVG_MPM_PD"].sum(min_count=1),
        "total_current_income": nns.loc[mask_non_by].groupby(lot)["NEW CURRENT"].sum(min_count=1),
        "total_sales_current": nns.loc[mask_non_by].groupby(lot)["Sales+Current"].sum(min_count=1),
        "sold_value": nns.loc[mask_non_by].groupby(lot)["Sold"].sum(min_count=1),
        "comp_value": nns.loc[mask_not_inprocess].groupby(lot)["AVG_MPM_PD"].sum(min_count=1),
        "stone_bank_value": nns.loc[mask_stone_bank].groupby(lot)["AVG_MPM_PD"].sum(min_count=1),
        "lab_received_value": nns.loc[mask_lab_received].groupby(lot)["AVG_MPM_PD"].sum(min_count=1),
        "r_emd_value": nns.loc[mask_r_emd].groupby(lot)["AVG_MPM_PD"].sum(min_count=1),
        "r_emd_loss_amount": nns.loc[nns["R-EMD LOSS PCS"]].groupby(lot)["MAX_CUR_FINAL_RATE_PD"].sum(min_count=1),
        "r_emd_gain_amount": nns.loc[nns["R-EMD GAIN PCS"]].groupby(lot)["MAX_CUR_FINAL_RATE_PD"].sum(min_count=1),
    }

    return aggregates


def aggregate_current_for_lot(current: pd.DataFrame) -> Dict[str, pd.Series]:
    if current.empty:
        keys = [
            "amount",
            "current_amount",
            "r_emd_loss_amount",
            "r_emd_gain_amount",
        ]
        return {key: pd.Series(dtype=float) for key in keys}

    lot = current["LotNo_key"]
    aggregates = {
        "amount": current.groupby(lot)["Amount"].sum(min_count=1),
        "current_amount": current.groupby(lot)["AF Curr PD Amt"].sum(min_count=1),
        "r_emd_loss_amount": current.loc[current["R-EMD LOSS PCS"]].groupby(lot)["AF Curr PD Amt"].sum(min_count=1),
        "r_emd_gain_amount": current.loc[current["R-EMD GAIN PCS"]].groupby(lot)["AF Curr PD Amt"].sum(min_count=1),
    }
    return aggregates


def aggregate_result_for_lot(result: pd.DataFrame) -> Dict[str, pd.Series]:
    if result.empty:
        keys = ["amount", "current_amount"]
        return {key: pd.Series(dtype=float) for key in keys}
    lot = result["LotNo_key"]
    aggregates = {
        "amount": result.groupby(lot)["Amount"].sum(min_count=1),
        "current_amount": result.groupby(lot)["AF Curr PD Amt"].sum(min_count=1),
    }
    return aggregates


# ---------------------------------------------------------------------------
# Rough detail calculation
# ---------------------------------------------------------------------------


def compute_rough_detail(
    rough: pd.DataFrame,
    lot_list: pd.DataFrame,
    multi_box: pd.DataFrame,
    or_uncut_agg: pd.DataFrame,
    direct_sales_agg: pd.DataFrame,
    damage_agg: pd.DataFrame,
    first_signer_agg: pd.DataFrame,
    nns: pd.DataFrame,
    nns_aggs: Dict[str, pd.Series],
    current_aggs: Dict[str, pd.Series],
    result_aggs: Dict[str, pd.Series],
    lot_check: pd.DataFrame,
) -> pd.DataFrame:
    if rough.empty:
        return rough

    rough = rough.copy()
    rough["LotNo_key"] = _normalise_string(rough["LotNo"] if "LotNo" in rough.columns else rough.iloc[:, 0])

    def get_numeric(column: str) -> pd.Series:
        if column in rough.columns:
            return pd.to_numeric(rough[column], errors="coerce").fillna(0.0)
        return pd.Series(0.0, index=rough.index)

    # Base references
    lot_code_map = {}
    if not lot_list.empty:
        lot_code_map = dict(zip(lot_list["LotNo_key"], lot_list["CODE"]))

    multi_box_map = {}
    if not multi_box.empty:
        multi_box_map = dict(zip(multi_box["LotNo_key"], multi_box["Multi Box Name"]))

    lot_check_set = set(lot_check["LotNo_key"]) if not lot_check.empty else set()

    # Merge aggregated tables via mapping
    def map_series(series: Optional[pd.Series]) -> pd.Series:
        if series is None or series.empty:
            return pd.Series(0.0, index=rough.index)
        return rough["LotNo_key"].map(series).fillna(0.0)

    or_uncut_map = {col: map_series(or_uncut_agg.set_index("LotNo_key")[col]) for col in ["OR UNCUT PCS", "OR UNCUT CTS", "OR UNCUT VALUE"] if col in or_uncut_agg.columns}
    for col in ["OR UNCUT PCS", "OR UNCUT CTS", "OR UNCUT VALUE"]:
        rough[col] = or_uncut_map.get(col, pd.Series(0.0, index=rough.index))

    direct_map = {col: map_series(direct_sales_agg.set_index("LotNo_key")[col]) for col in ["DIRECT SALES PCs", "DIRECT SALES CTS", "DIRECT SALES VALUE"] if col in direct_sales_agg.columns}
    for col in ["DIRECT SALES PCs", "DIRECT SALES CTS", "DIRECT SALES VALUE"]:
        rough[col] = direct_map.get(col, pd.Series(0.0, index=rough.index))

    if not damage_agg.empty:
        damage_indexed = damage_agg.set_index("LotNo_key")
        for col, new_name in zip(["BREAKAGE", "WORKER MISTAKE", "GRAND TOTAL", "COMPROMISE"], ["BREAKAGE", "WORKER MISTAKE", "Total Damage", "COMPROMISE"]):
            rough[new_name] = map_series(damage_indexed[col]) if col in damage_indexed.columns else 0.0
    else:
        for new_name in ["BREAKAGE", "WORKER MISTAKE", "Total Damage", "COMPROMISE"]:
            rough[new_name] = 0.0

    if not first_signer_agg.empty:
        rough["1st Result Uncut Adjustment"] = map_series(first_signer_agg.set_index("LotNo_key")["1st Result Uncut Adjustment"])
    else:
        rough["1st Result Uncut Adjustment"] = 0.0

    # Derived values from reference tables
    org_cts = get_numeric("OrgCts")
    labour_rate = get_numeric("LABOUR")
    rough["OrgCts"] = org_cts
    rough["LABOUR"] = labour_rate

    rough["Code Value"] = rough["LotNo_key"].map(lot_code_map).fillna(0.0) * org_cts

    box_counts = rough.groupby("Box")["Box"].transform("count") if "Box" in rough.columns else pd.Series(1, index=rough.index)
    base_name = _strip_string(rough.get("Article_For_Report", pd.Series("", index=rough.index)))
    fallback_name = _strip_string(rough.get("Article Group", pd.Series("", index=rough.index)))
    base_name = np.where(base_name == "", fallback_name, base_name)
    rough["NEW BOX"] = np.where(box_counts > 1, base_name + "#" + _strip_string(rough.get("Box", pd.Series("", index=rough.index))), "")

    rough["Total"] = rough["LotNo_key"].map(multi_box_map).fillna("")

    # Aggregations from NNS
    by_product = map_series(nns_aggs["by_product_mpm"])
    total_income = map_series(nns_aggs["total_income"])
    total_current_income = map_series(nns_aggs["total_current_income"])
    total_sales_current = map_series(nns_aggs["total_sales_current"])
    sold_value = map_series(nns_aggs["sold_value"])
    comp_value = map_series(nns_aggs["comp_value"])
    stone_bank_value = map_series(nns_aggs["stone_bank_value"])
    lab_received_value = map_series(nns_aggs["lab_received_value"])
    r_emd_value = map_series(nns_aggs["r_emd_value"])
    nns_r_emd_loss = map_series(nns_aggs["r_emd_loss_amount"])
    nns_r_emd_gain = map_series(nns_aggs["r_emd_gain_amount"])

    rough["By Product"] = by_product - rough["OR UNCUT VALUE"].fillna(0.0)
    rough["Total Income"] = total_income

    labour_value = (org_cts - rough["OR UNCUT CTS"] - rough["DIRECT SALES CTS"]) * labour_rate
    rough["Labour Value"] = labour_value

    direct_sales_value = rough["DIRECT SALES VALUE"].fillna(0.0)

    rough["Total Income-Expense"] = (rough["Total Income"] + rough["By Product"] - labour_value) * 0.96 + rough["OR UNCUT VALUE"] + direct_sales_value

    rough["Total Current Income"] = total_current_income
    rough["Total Current Income-Expense"] = (rough["Total Current Income"] + rough["By Product"] - labour_value) * 0.96 + rough["OR UNCUT VALUE"] + direct_sales_value

    rough["Sales+Current"] = total_sales_current
    rough["Total Sales"] = (rough["Sales+Current"] + rough["By Product"] - labour_value) * 0.96 + rough["OR UNCUT VALUE"] + direct_sales_value
    rough["Sold Value"] = sold_value

    rough["Comp Value"] = comp_value
    rough["Stone Bank Value"] = stone_bank_value
    rough["LabRecived Value"] = lab_received_value
    rough["R-EMD VALUE"] = r_emd_value

    # Additional derived metrics
    rough["Expense"] = (rough["Total Income"] - labour_value + rough["By Product"]) * 0.04
    rough["nns lots"] = rough["LotNo_key"].isin(nns["DISP_LOTNO_key"].unique()) if not nns.empty else False

    # Current & Result Get Rate
    current_amount = map_series(current_aggs["amount"])
    current_current_amount = map_series(current_aggs["current_amount"])
    current_r_emd_loss = map_series(current_aggs["r_emd_loss_amount"])
    current_r_emd_gain = map_series(current_aggs["r_emd_gain_amount"])

    result_amount = map_series(result_aggs["amount"])
    result_current_amount = map_series(result_aggs["current_amount"])

    rough["Get Rate Today Amount"] = current_amount
    rough["Get Rate Today Current Amount"] = current_current_amount
    rough["Get Rate Result Amount Check"] = result_amount
    rough["Get Rate Result Amount"] = np.where(np.isclose(result_amount, 0.0), current_amount, result_amount)
    rough["Get Rate Result Current Amount"] = np.where(
        np.isclose(result_amount, 0.0), current_current_amount, result_current_amount
    )

    rough["R-EMD Loss Value"] = nns_r_emd_loss - current_r_emd_loss
    rough["R-EMD Gain Value"] = nns_r_emd_gain - current_r_emd_gain
    rough["R-EMD Effect Value"] = np.where(
        np.isclose(rough["R-EMD Loss Value"], 0.0), rough["R-EMD Gain Value"], rough["R-EMD Loss Value"]
    )

    rough["R-EMD CONVERT LOSS%"] = -0.20
    rough["APPROX R-EMD CONVERT LOSS VALUE"] = -np.abs(rough["R-EMD CONVERT LOSS%"]) * rough["R-EMD VALUE"]

    rough["DIRECT SALES PCs"] = rough["DIRECT SALES PCs"]

    rough["Lot number"] = rough["LotNo"].apply(lambda x: _extract_numeric_prefix(str(x) if not pd.isna(x) else ""))

    labour_org = labour_rate
    adjustment = rough["1st Result Uncut Adjustment"].fillna(0.0)
    rough["1st Result Income - Expense"] = (rough["Get Rate Today Amount"] + adjustment - labour_org * org_cts) * 0.96
    rough["1st Result Current Income - Expense"] = (
        rough["Get Rate Today Current Amount"] + adjustment - labour_org * org_cts
    ) * 0.96

    rough["Total Income + By Product"] = rough["Total Income"] + rough["By Product"]
    rough["Total Sales+Current + By Product"] = rough["By Product"] + rough["Sales+Current"]

    # UNHIDE flag
    rough["UNHIDE"] = rough["LotNo_key"].isin(lot_check_set)

    # Office
    lotno_str = _strip_string(rough["LotNo"])
    rough["OFFICE"] = np.where(lotno_str.str.startswith("2"), "BTS", "SRT")

    # Box date / Month calculations
    trans_dates = pd.to_datetime(rough.get("TransDate"), errors="coerce")
    rough["TransDate"] = trans_dates

    def compute_box_dates(df: pd.DataFrame) -> pd.Series:
        result = pd.Series(np.nan, index=df.index, dtype="datetime64[ns]")
        srt_mask = df["OFFICE"] == "SRT"
        srt_df = df.loc[srt_mask]

        if not srt_df.empty:
            total_means = srt_df.groupby("Total")["TransDate"].transform(_safe_mean)
            box_means = srt_df.groupby("Box")["TransDate"].transform(_safe_mean) if "Box" in df.columns else pd.Series(np.nan, index=srt_df.index)
            result.loc[srt_mask] = np.where(
                _strip_string(srt_df["Total"]).values != "",
                total_means,
                box_means,
            )

        bts_mask = df["OFFICE"] == "BTS"
        bts_df = df.loc[bts_mask]
        if not bts_df.empty:
            box_means_all = bts_df.groupby("Box")["TransDate"].transform(_safe_mean) if "Box" in df.columns else pd.Series(np.nan, index=bts_df.index)
            result.loc[bts_mask] = box_means_all

        return pd.to_datetime(result, errors="coerce")

    rough["Box Date"] = compute_box_dates(rough)
    rough["Month"] = rough["Box Date"].dt.to_period("M").dt.to_timestamp()

    rough = rough.drop(columns=[col for col in ["LotNo_key"] if col in rough.columns])

    base_columns = [
        "LotNo",
        "OrgPcs",
        "OrgCts",
        "TransDate",
        "SubRghType",
        "LABOUR",
        "Box",
        "Mine",
        "Country",
        "BoxDesc",
        "SightMonth",
        "Article_For_Report",
        "DTC_NONDTC",
        "Article Group",
    ]

    derived_columns = [
        "NEW BOX",
        "Code Value",
        "OR UNCUT PCS",
        "OR UNCUT CTS",
        "OR UNCUT VALUE",
        "By Product",
        "Total Income",
        "Total Income-Expense",
        "Total Current Income",
        "Total Current Income-Expense",
        "Total Sales",
        "Sold Value",
        "Sales+Current",
        "Comp Value",
        "Stone Bank Value",
        "Total",
        "Get Rate Result Amount",
        "Get Rate Result Current Amount",
        "Get Rate Today Amount",
        "Get Rate Today Current Amount",
        "Get Rate Result Amount Check",
        "UNHIDE",
        "Month",
        "Labour Value",
        "Expense",
        "nns lots",
        "LabRecived Value",
        "DIRECT SALES VALUE",
        "DIRECT SALES CTS",
        "DIRECT SALES PCs",
        "OFFICE",
        "R-EMD VALUE",
        "R-EMD CONVERT LOSS%",
        "APPROX R-EMD CONVERT LOSS VALUE",
        "Lot number",
        "1st Result Uncut Adjustment",
        "1st Result Income - Expense",
        "1st Result Current Income - Expense",
        "R-EMD Loss Value",
        "R-EMD Gain Value",
        "R-EMD Effect Value",
        "BREAKAGE",
        "WORKER MISTAKE",
        "Total Damage",
        "COMPROMISE",
        "Total Income + By Product",
        "Total Sales+Current + By Product",
        "Box Date",
    ]

    column_order = [col for col in base_columns + derived_columns if col in rough.columns]
    column_order += [col for col in rough.columns if col not in column_order]

    return rough.loc[:, column_order]


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------


def main() -> None:
    # File paths
    lot_list = load_lot_list("LOT LIST.xlsx")
    multi_box = load_multi_box("Multi Box List.xlsx")
    sales = load_sales_data("Sales Data 14-11-2025.xlsx")
    or_uncut = load_or_uncut("OR UNCUT -14-11-2025.xlsx")
    direct_sales = load_direct_sales("DS14-11-2025.xlsx")
    damage = load_damage("Damage Detail 14-11-2025.xlsx")
    first_signer = load_first_signer("1st Signer Missing Data.xlsx")
    rough_detail = _load_excel("Rough Detail 14-11-2025.xlsx")
    nns = load_nns_data("NNS Data.xlsx")
    current = load_current_getrate("715b530d-e25c-463b-8b3d-4c2b249ff692.xlsx")
    result = load_result_getrate("Result Get Rate 14-11-2025.xlsx")
    lot_check = load_lot_check(["LOT CHECK.xlsx", "Lot Check.xlsx", "lot_check.xlsx"])

    sales_map = enrich_sales_mapping(sales)

    # Initial R-EMD lookup from NNS (before current enrichment)
    if not nns.empty:
        remd_flags = (
            nns.assign(_remd=(_strip_string(nns["SHP"]).str.upper() == "R-EMD"))
            .groupby(["DISP_LOTNO_key", "MAIN_PKTNO_key"])["_remd"]
            .transform("any")
        )
        nns_remd_lookup = {
            (lot, main): bool(flag)
            for (lot, main), flag in zip(zip(nns["DISP_LOTNO_key"], nns["MAIN_PKTNO_key"]), remd_flags)
        }
    else:
        nns_remd_lookup = {}

    current, current_lookup = enrich_current_getrate(current, nns_remd_lookup)
    nns = enrich_nns_data(nns, sales_map, current_lookup)
    result = enrich_result_getrate(result)

    or_uncut_agg = aggregate_or_uncut(or_uncut)
    direct_sales_agg = aggregate_direct_sales(direct_sales)
    damage_agg = aggregate_damage(damage)
    first_signer_agg = aggregate_first_signer(first_signer)
    nns_aggs = aggregate_nns_for_lot(nns)
    current_aggs = aggregate_current_for_lot(current)
    result_aggs = aggregate_result_for_lot(result)

    rough_calculated = compute_rough_detail(
        rough_detail,
        lot_list,
        multi_box,
        or_uncut_agg,
        direct_sales_agg,
        damage_agg,
        first_signer_agg,
        nns,
        nns_aggs,
        current_aggs,
        result_aggs,
        lot_check,
    )

    if rough_calculated.empty:
        print("No rough detail data available to process.")
        return

    output_path = "Rough_Detail_Calculated.xlsx"
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        rough_calculated.to_excel(writer, sheet_name="Rough_Detail", index=False)

        # Optional helper sheets for auditing
        if not nns.empty:
            nns.to_excel(writer, sheet_name="NNS_Enriched", index=False)
        if not current.empty:
            current.to_excel(writer, sheet_name="Current_GetRate", index=False)
        if not result.empty:
            result.to_excel(writer, sheet_name="Result_GetRate", index=False)

    print(f"Rough detail written to '{output_path}'.")


if __name__ == "__main__":
    main()

