"""Command-line tool for reproducing the VRS processing pipeline.

This script converts the original Google Colab notebook into a reusable
Python program that can be executed locally (for example from VS Code).
Provide the paths to the main workbook and the Lot Master workbook; the
script will regenerate all intermediate tables and write them to a single
Excel file named ``processed_data_summary.xlsx`` by default.

Example:

    python process_vrs_report.py --main /path/to/main.xlsx \
        --lot-master /path/to/lot_master.xlsx --output output.xlsx
"""
from __future__ import annotations

import argparse
import io
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def normalize_name(name: str) -> str:
    """Lowercase and strip non-alphanumeric characters from a string."""

    return re.sub(r"[^a-zA-Z0-9]", "", str(name)).lower()


def find_sheet_name(xls: pd.ExcelFile, target: str) -> str:
    """Return the actual sheet name that matches *target* after normalisation."""

    target_norm = normalize_name(target)
    for sheet_name in xls.sheet_names:
        if normalize_name(sheet_name) == target_norm:
            return sheet_name
    raise ValueError(
        f"Sheet '{target}' (or a similar variant) not found in '{xls.io}'."
    )


def find_column(df: pd.DataFrame, target: str) -> Optional[str]:
    """Locate a column in ``df`` matching *target* after normalisation."""

    target_norm = normalize_name(target)
    for col in df.columns:
        if normalize_name(col) == target_norm:
            return col
    return None


def extract_numeric_prefix(value: object) -> Optional[str]:
    """Return the leading numeric portion of a value (as a string)."""

    match = re.match(r"^(\d+)", str(value)) if value is not None else None
    return match.group(0) if match else None


def to_nullable_int(series: pd.Series) -> pd.Series:
    """Convert a series to pandas nullable integer using numeric prefixes."""

    return (
        series.astype(str)
        .apply(lambda x: re.match(r"^\d+", x).group(0) if re.match(r"^\d+", x) else None)
        .astype(pd.Int64Dtype())
    )


# ---------------------------------------------------------------------------
# Data classes for inputs
# ---------------------------------------------------------------------------


@dataclass
class WorkbookSources:
    main: Path
    lot_master: Path
    output: Path


# ---------------------------------------------------------------------------
# Processing steps
# ---------------------------------------------------------------------------


def process_lot_list(xls: pd.ExcelFile) -> pd.DataFrame:
    sheet_name = find_sheet_name(xls, "LOT LIST")
    df = xls.parse(sheet_name)

    lot_col = find_column(df, "LOTNO")
    code_col = find_column(df, "CODE")
    if lot_col is None or code_col is None:
        missing = [name for name, col in [("LOTNO", lot_col), ("CODE", code_col)] if col is None]
        raise KeyError(f"Required columns {missing} not found in sheet '{sheet_name}'.")

    df = df.copy()
    initial_rows = len(df)
    df.drop_duplicates(subset=[lot_col], keep="first", inplace=True)
    if len(df) != initial_rows:
        removed_rows = initial_rows - len(df)
        print(f"Removed {removed_rows} duplicate entries from '{lot_col}' in '{sheet_name}'.")

    df["LOT NUMERIC"] = to_nullable_int(df[lot_col])
    df["COGS POINT"] = pd.to_numeric(df[code_col], errors="coerce") / 2
    df.rename(columns={lot_col: "LotNo", code_col: "CODE"}, inplace=True)
    return df


def process_multi_box(xls: pd.ExcelFile) -> pd.DataFrame:
    sheet_name = find_sheet_name(xls, "Multi Box")
    df = xls.parse(sheet_name)

    lot_col = None
    for candidate in ("Lot", "Lotno"):
        lot_col = find_column(df, candidate)
        if lot_col:
            break
    name_col = find_column(df, "Multi Box Name")
    if lot_col is None or name_col is None:
        raise KeyError("Multi Box sheet requires Lot/Lotno and Multi Box Name columns.")

    df = df.copy()
    df["LOT NUMERIC"] = to_nullable_int(df[lot_col])
    df.rename(columns={lot_col: "Lot", name_col: "Multi Box Name"}, inplace=True)
    return df


def process_or_uncut(xls: pd.ExcelFile) -> pd.DataFrame:
    sheet_name = find_sheet_name(xls, "OR UNCUT")
    df = xls.parse(sheet_name)

    cts_col = find_column(df, "Cts")
    rate_col = find_column(df, "Rate")
    if cts_col is None or rate_col is None:
        missing = [name for name, col in [("Cts", cts_col), ("Rate", rate_col)] if col is None]
        raise KeyError(f"Required columns {missing} not found in sheet '{sheet_name}'.")

    df = df.copy()
    df["Cts_Numeric"] = pd.to_numeric(df[cts_col], errors="coerce")
    df["Rate_Numeric"] = pd.to_numeric(df[rate_col], errors="coerce")
    df["Amount"] = df["Cts_Numeric"] * df["Rate_Numeric"]
    df.rename(columns={cts_col: "Cts", rate_col: "Rate"}, inplace=True)
    return df


def process_direct_sales(xls: pd.ExcelFile) -> pd.DataFrame:
    sheet_name = find_sheet_name(xls, "DIRECT SALES")
    df = xls.parse(sheet_name)

    cts_col = find_column(df, "OR UNCUT CTS")
    per_cts_col = find_column(df, "Per Cts")
    lot_col = find_column(df, "Lot No")
    if None in (cts_col, per_cts_col, lot_col):
        missing = [name for name, col in [("OR UNCUT CTS", cts_col), ("Per Cts", per_cts_col), ("Lot No", lot_col)] if col is None]
        raise KeyError(f"Required columns {missing} not found in sheet '{sheet_name}'.")

    df = df.copy()
    df["OR UNCUT CTS_Numeric"] = pd.to_numeric(df[cts_col], errors="coerce")
    df["Per Cts_Numeric"] = pd.to_numeric(df[per_cts_col], errors="coerce")
    df["OR UNCUT VALUE"] = df["OR UNCUT CTS_Numeric"] * df["Per Cts_Numeric"]
    df["Lotno"] = to_nullable_int(df[lot_col])
    df.rename(columns={cts_col: "OR UNCUT CTS", per_cts_col: "Per Cts"}, inplace=True)
    return df


def update_lotno_and_disp_lotno(damage_df: pd.DataFrame, lotmaster_df: pd.DataFrame) -> pd.DataFrame:
    damage_df = damage_df.copy()
    lotmaster_df = lotmaster_df.copy()

    damage_df.columns = [str(c).strip() for c in damage_df.columns]
    lotmaster_df.columns = [str(c).strip() for c in lotmaster_df.columns]

    for required in ("LOTNO", "DISP_LOTNO"):
        if required not in damage_df.columns:
            raise KeyError("DAMAGE sheet must contain 'LOTNO' and 'DISP_LOTNO' columns.")
    for required in ("LOTNO", "DISP_LOTNO"):
        if required not in lotmaster_df.columns:
            raise KeyError("LOT MASTER sheet must contain 'LOTNO' and 'DISP_LOTNO' columns.")

    damage_df["EXTRACT"] = damage_df["DISP_LOTNO"].apply(lambda v: extract_numeric_prefix(v) if pd.notna(v) else None)
    damage_df["LOTNO"] = damage_df.apply(
        lambda row: row["EXTRACT"] if pd.notna(row["EXTRACT"]) else row["LOTNO"], axis=1
    )

    def normalize_lotno(x: object) -> Optional[str]:
        if pd.isna(x):
            return None
        s = str(x).strip()
        if re.match(r"^\d+\.0$", s):
            return s.split(".")[0]
        if re.match(r"^\d+$", s):
            return s
        return s

    damage_df["LOT_KEY"] = damage_df["LOTNO"].apply(normalize_lotno)
    lotmaster_df["LOT_KEY"] = lotmaster_df["LOTNO"].apply(normalize_lotno)

    lotmaster_map = (
        lotmaster_df.dropna(subset=["LOT_KEY"])
        .set_index("LOT_KEY")["DISP_LOTNO"]
        .astype(str)
    )

    damage_df["DISP_LOTNO"] = damage_df["LOT_KEY"].map(lotmaster_map).fillna(damage_df["DISP_LOTNO"])
    return damage_df.drop(columns=["EXTRACT", "LOT_KEY"])


def process_damage_values_from_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    required_cols = [
        "LOTNO",
        "DISP_LOTNO",
        "REVIEWED_RATE_DIFF",
        "RATE_DIFF",
        "PEN_INCEN",
        "BREAKAGEDUETO",
        "DEMAGE_REMARK",
        "REASON",
        "REVIEW_FLG",
        "SUBREASON",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    lotno_non_empty = df["LOTNO"].notna() & (df["LOTNO"].astype(str).str.strip() != "")
    if lotno_non_empty.any():
        last_valid_index = lotno_non_empty[lotno_non_empty].index[-1]
        df = df.loc[:last_valid_index].copy()

    reviewed = df["REVIEWED_RATE_DIFF"]
    reviewed_empty = reviewed.isna() | (reviewed.astype(str).str.strip() == "")
    df.loc[reviewed_empty, "REVIEWED_RATE_DIFF"] = df.loc[reviewed_empty, "RATE_DIFF"]

    df = df[~df["PEN_INCEN"].astype(str).str.upper().isin(["INCENTIVE", "REPAIR"])]

    damage_df = df.copy()
    compromise_df = df.copy()

    damage_df = damage_df[
        ~damage_df["BREAKAGEDUETO"].astype(str).str.upper().isin(["TENSION", "QC ERROR"])
    ]

    demage_exclude = {
        "CROSS ENTRY",
        "GRADER MISTAKE",
        "LAB RECUT",
        "UPGRADE",
        "CROSS GRADER MISTAKE",
        "25%",
        "100%",
        "75%",
        "50%",
        "POINTED",
    }
    damage_df = damage_df[
        ~damage_df["DEMAGE_REMARK"].astype(str).str.upper().isin(demage_exclude)
    ]

    reason_exclude = {"GRADER MISTAKE", "PLAN MISSED", "QC ERROR", "CROSS ENTRY", "UPGRADE"}
    damage_df = damage_df[
        ~damage_df["REASON"].astype(str).str.upper().isin(reason_exclude)
    ]

    review_flg_exclude = {"R-0 %", "R-5 %", "R-1 %", "R-2 %"}
    damage_df = damage_df[
        ~damage_df["REVIEW_FLG"].astype(str).str.upper().isin(review_flg_exclude)
    ]

    subreason_exclude = {"UPGRADE", "CROSS ENTRY", "GRADER MISTAKE"}
    damage_df = damage_df[
        ~damage_df["SUBREASON"].astype(str).str.upper().isin(subreason_exclude)
    ]

    compromise_df = compromise_df[
        compromise_df["REASON"].astype(str).str.upper() == "COMPROMISE"
    ]

    for temp_df in (damage_df, compromise_df):
        temp_df["REVIEWED_RATE_DIFF_NUM"] = pd.to_numeric(temp_df["REVIEWED_RATE_DIFF"], errors="coerce")

    damage_pivot = pd.pivot_table(
        damage_df,
        index="DISP_LOTNO",
        columns="BREAKAGEDUETO",
        values="REVIEWED_RATE_DIFF_NUM",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    compromise_pivot = (
        compromise_df.groupby("DISP_LOTNO", as_index=False)["REVIEWED_RATE_DIFF_NUM"]
        .sum()
        .rename(columns={"REVIEWED_RATE_DIFF_NUM": "COMPROMISE"})
    )

    final = damage_pivot[["DISP_LOTNO"]].copy()
    final["BREAKAGE"] = damage_pivot.get("BREAKAGE", 0.0)
    final["WORKER MISTAKE"] = damage_pivot.get("WORKER MISTAKE", 0.0)
    final[["BREAKAGE", "WORKER MISTAKE"]] = final[["BREAKAGE", "WORKER MISTAKE"]].fillna(0.0)
    final["Grand Total"] = final["BREAKAGE"] + final["WORKER MISTAKE"]
    final = final.merge(compromise_pivot, on="DISP_LOTNO", how="left")
    final["COMPROMISE"] = final["COMPROMISE"].fillna(0.0)
    final = final.rename(columns={"DISP_LOTNO": "LOTNO"})
    return final[["LOTNO", "BREAKAGE", "WORKER MISTAKE", "Grand Total", "COMPROMISE"]]


def load_damage(xls_main: pd.ExcelFile, xls_lot_master: pd.ExcelFile) -> pd.DataFrame:
    damage_sheet = None
    for candidate in ("DAMAGE", "DAMAGE DETAIL"):
        try:
            damage_sheet = find_sheet_name(xls_main, candidate)
            break
        except ValueError:
            continue
    if damage_sheet is None:
        raise RuntimeError("Neither 'DAMAGE' nor 'DAMAGE DETAIL' sheet found in main workbook.")

    damage_df_raw = xls_main.parse(damage_sheet)
    lot_master_sheet = xls_lot_master.sheet_names[0]
    lotmaster_df = xls_lot_master.parse(lot_master_sheet)

    updated = update_lotno_and_disp_lotno(damage_df_raw, lotmaster_df)
    return process_damage_values_from_df(updated)


def load_simple_sheet(xls: pd.ExcelFile, target_name: str) -> pd.DataFrame:
    sheet_name = find_sheet_name(xls, target_name)
    return xls.parse(sheet_name)


# ---------------------------------------------------------------------------
# Harshbhai sales helpers
# ---------------------------------------------------------------------------


def gia_exp(cts: float) -> Optional[float]:
    if not cts or cts <= 0:
        return None
    for th, fee in [
        (50, "Check"),
        (40, 4039.14),
        (30, 3412.56),
        (25, 2816.66),
        (20, 2417.82),
        (15, 1856.14),
        (12, 1523.38),
        (10, 1292.1),
        (8, 934.56),
        (6, 789.42),
        (5, 670.24),
        (4, 472),
        (3, 352.82),
        (2, 241.9),
        (1.5, 173.46),
        (1.2, 154.58),
        (1, 148.68),
        (0.7, 61.36),
        (0.5, 50.74),
        (0.4, 49.56),
        (0.3, 47.2),
        (0.23, 38.94),
        (0.15, 36.58),
    ]:
        if cts >= th:
            return fee if isinstance(fee, (int, float)) else None
    return None


def hrd_exp(cts: float) -> Optional[float]:
    if not cts or cts <= 0:
        return None
    if cts < 0.5:
        return 53.17
    if cts < 0.7:
        return 67.06
    if cts < 0.9:
        return 74.01
    if cts < 1.0:
        return 84.44
    if cts < 1.5:
        return 118
    if cts < 2.0:
        return 142.32
    if cts < 3.0:
        return 194.44
    if cts < 4.0:
        return 263.93
    return None


def igi_exp(cts: float) -> Optional[float]:
    if not cts or cts <= 0:
        return None
    if 0.001 <= cts < 0.23:
        return round(2277 / 55)
    if 0.23 <= cts < 0.30:
        return round(2829 / 55)
    if 0.30 <= cts < 0.50:
        return round(3243 / 55)
    if 0.50 <= cts < 0.70:
        return round(3588 / 55)
    if 0.70 <= cts < 1.00:
        return round(4002 / 55)
    if 1.00 <= cts < 5.00:
        c = 3660 * cts + 720
        return round((c * 1.15) / 55)
    if 5.00 <= cts < 100:
        c1 = 5580 * (cts - 4.999)
        c = 3660 * 4.999 + c1 + 720
        return round((c * 1.15) / 55)
    return None


def lab_expense_per_pcs(lab: str, cts: float) -> Optional[float]:
    lab = (lab or "").upper()
    if lab == "" or lab == "NC":
        base = 0
    elif any(x in lab for x in ("GIA", "DB", "FM")):
        base = gia_exp(cts)
    elif any(x in lab for x in ("GSI", "HRD")):
        base = hrd_exp(cts)
    elif "IGI" in lab:
        base = igi_exp(cts)
    else:
        base = 0
    return base if isinstance(base, (int, float)) else gia_exp(cts)


def process_harshbhai_sales(xls: pd.ExcelFile) -> pd.DataFrame:
    sheet_name = find_sheet_name(xls, "Harshbhai Sales")
    df_sales = xls.parse(sheet_name)

    selected_columns = ["Stone Id", "Party Amt", "Lab", "cts"]
    for col in selected_columns:
        if find_column(df_sales, col) is None:
            raise KeyError(f"Required column '{col}' not found in '{sheet_name}'.")

    df_sales = df_sales.copy()
    df_sales.rename(columns={find_column(df_sales, col): col for col in selected_columns}, inplace=True)

    df_sales_processed = df_sales[selected_columns].copy()
    df_sales_processed["Lab Expense"] = df_sales_processed.apply(
        lambda row: lab_expense_per_pcs(str(row["Lab"]), row["cts"]), axis=1
    )
    df_sales_processed["Sales Value"] = df_sales_processed["Party Amt"] - df_sales_processed["Lab Expense"].fillna(0)
    df_sales_processed["Harshbhai Sales"] = df_sales_processed["Party Amt"]
    return df_sales_processed


def process_result_get_rate(xls: pd.ExcelFile) -> pd.DataFrame:
    sheet_name = find_sheet_name(xls, "Result Get Rate")
    df = xls.parse(sheet_name)

    df = df.copy()
    if "Lotno" in df.columns:
        df["Lotno2"] = to_nullable_int(df["Lotno"])
    else:
        df["Lotno2"] = pd.Series(dtype=pd.Int64Dtype())

    if "main Pktno" in df.columns:
        df["main Pktno_Int"] = pd.to_numeric(df["main Pktno"], errors="coerce").astype(pd.Int64Dtype())
        df["main Pktno_str"] = df["main Pktno_Int"].astype(str).replace("<NA>", "")
    else:
        df["main Pktno_Int"] = pd.Series(dtype=pd.Int64Dtype())
        df["main Pktno_str"] = ""

    df["Key"] = df["Lotno2"].astype(str).replace("<NA>", "") + "#" + df["main Pktno_str"]
    df["P KEY"] = (
        df.get("Lotno", pd.Series(dtype="object")).astype(str).replace("nan", "")
        + "#" + df["main Pktno_str"]
        + "#" + df.get("Tag", pd.Series(dtype="object")).astype(str).replace("nan", "")
    )
    df["Lotno3"] = (
        df.get("Lotno", pd.Series(dtype="object")).astype(str).replace("nan", "")
        + "#" + df["main Pktno_str"]
        + "#" + df.get("Tag", pd.Series(dtype="object")).astype(str).replace("nan", "")
    )
    return df


def process_current_get_rate(xls: pd.ExcelFile) -> pd.DataFrame:
    sheet_name = find_sheet_name(xls, "CURRENT GET RATE")
    df = xls.parse(sheet_name)

    df = df.copy()
    df.drop(columns=["R-EMD LOSS PCS", "R-EMD GAIN PCS"], errors="ignore", inplace=True)

    df["Lotno"] = df["Lotno"].astype(str)
    df["Lotno2"] = to_nullable_int(df["Lotno"])

    main_pkt_col = "Main  Pktno" if "Main  Pktno" in df.columns else "Main Pktno"
    if main_pkt_col not in df.columns:
        raise KeyError("CURRENT GET RATE sheet must contain 'Main  Pktno' column.")

    df["Main_Pktno_Int"] = pd.to_numeric(df[main_pkt_col], errors="coerce").astype(pd.Int64Dtype())
    df["Main_Pktno_str"] = df["Main_Pktno_Int"].astype(str).replace("<NA>", "")
    df["Pktno_Tag_str"] = df.get("Tag", pd.Series(dtype="object")).astype(str).replace("nan", "")
    df["Lotno_str"] = df["Lotno"].astype(str).replace("nan", "")

    df["KEY"] = df["Lotno2"].astype(str).replace("<NA>", "") + "#" + df["Main_Pktno_str"]
    df["P KEY"] = df["Lotno_str"] + "#" + df["Main_Pktno_str"] + "#" + df["Pktno_Tag_str"]

    df["R-EMD PCS"] = (
        df.groupby(["Lotno", "Main_Pktno_Int"]) ["Shape"].transform(lambda x: (x.astype(str).str.upper() == "R-EMD").any())
    )

    duplicated_p_keys = df[df["P KEY"].duplicated(keep=False)]["P KEY"].unique()
    lotnos_with_dup = df[df["P KEY"].isin(duplicated_p_keys)]["Lotno"].unique()
    df["DUPLICATE"] = df["Lotno"].isin(lotnos_with_dup)
    return df


def process_result_data(xls: pd.ExcelFile) -> pd.DataFrame:
    return load_simple_sheet(xls, "RESULT DATA")


def process_rough_detail(xls: pd.ExcelFile) -> pd.DataFrame:
    return load_simple_sheet(xls, "Rough Detail")


# ---------------------------------------------------------------------------
# NNS calculation
# ---------------------------------------------------------------------------


def calculate_nns_result(
    df_nns_data: pd.DataFrame,
    lot_list: pd.DataFrame,
    rough_detail: pd.DataFrame,
    df_current_getrate: pd.DataFrame,
    df_sales_processed: pd.DataFrame,
) -> pd.DataFrame:
    original_95_columns = [
        "DISP_LOTNO",
        "PKTNO",
        "MAIN_PKTNO",
        "PACKETID",
        "SIZE_NAME",
        "ROUGH_UNCUT",
        "LBR_RATE",
        "ORG_CTS",
        "MISTAKE_VAL",
        "TAG_NAME",
        "STONE_STATUS",
        "POL_TYPE",
        "OS_STATUS",
        "STONE_ID",
        "REMD_LESS_PER",
        "FANCY_CLR",
        "LAB",
        "CTS",
        "SHP",
        "CLA",
        "COL",
        "CUT",
        "NEW_CUT",
        "POL",
        "SYM",
        "FLR",
        "HGT",
        "TBL",
        "DIAMETER",
        "LEN",
        "WDTH",
        "DEPTH_MM",
        "CR_ANG",
        "PV_ANG",
        "CR_HGT",
        "PV_HGT",
        "GIRDLE",
        "L_W",
        "PROG_NAME",
        "RAPO_RATE",
        "MPM",
        "PD_PER",
        "PD",
        "PD_CNT",
        "MPM_PD",
        "RGH_COST_VAL",
        "FINAL_COST",
        "DIFF",
        "CUR_OLD_EXP_RATE",
        "CUR_NEW_RAPO_RATE",
        "CUR_NEW_EXP_RATE",
        "CUR_NEW_DISC_PER",
        "CUR_FINAL_RATE",
        "CUR_FINAL_RATE_PD",
        "CUR_PD",
        "DISCOVER_FLG",
        "CUR_PROG_NAME",
        "RATE_TYPE",
        "CUR_PROG_RATE",
        "CURR_PROG_NAME",
        "PUR_AMT",
        "ARTICLE",
        "LOTNO",
        "SHAPE_GROUP",
        "CLARITY_GROUP",
        "COLOR_GROUP",
        "SIZE_GROUP",
        "TRANS_TYPE",
        "AVG_VALUE",
        "AVG_MPM_PD",
        "STONE_CATEGORY",
        "SIDTYPE",
        "RATE_TYPE_CUR_FINAL_RATE",
        "MAX_CUR_FINAL_RATE_PD",
        "SI2_I1",
        "SALES_AVG_VALUE",
        "RATE_ENTRY",
        "DISC_PER",
        "ISACTIVE_PROG",
        "S_LAB_STD_EXP",
        "PROG_AMOUNT",
        "YNS_STATUS",
        "LSD_SCORE",
        "LSD_AMT",
        "CSD_SCORE",
        "CSD_CUR_AMOUNT",
        "SELLABILITY_RANK",
        "SALES_CALC_VALUE",
        "IS_SALES_VALUE",
        "IS_UPD_FLG",
        "IO_DATE",
        "UPD_DATE",
        "ENT_DATE",
        "ENT_USER",
        "ENT_TERM",
    ]

    missing_base = [c for c in original_95_columns if c not in df_nns_data.columns]
    if missing_base:
        raise ValueError(f"Missing base columns in NNS/RESULT sheet: {missing_base}")

    df_nns_data = df_nns_data[original_95_columns].copy()

    new_columns = [
        "Key",
        "PRICE CHANGE",
        "NEW CURRENT",
        "COUNT",
        "Sold",
        "Sales+Current",
        "AVG_MPM_PD+By Product",
        "Running %",
        "Rough Cost",
        "Labour",
        "Sales",
        "P & L",
        "P & L %",
        "Comp%",
        "Lot Status",
        "Inv Status",
        "Month",
        "DTC_NON DTC",
        "Include Lots",
        "Harshbhai Sales",
        "Lab Expense",
        "Harshbhai Sales Status",
        "Rel",
        "PKT KEY",
        "New Pktno",
        "Duplicate",
    ]
    for col in new_columns:
        df_nns_data[col] = np.nan
    df_nns_data["Polish Yield%"] = np.nan
    df_nns_data["R-EMD PCS"] = np.nan

    for col in [
        "DISP_LOTNO",
        "MAIN_PKTNO",
        "TAG_NAME",
        "RATE_TYPE",
        "STONE_ID",
        "SIDTYPE",
        "STONE_STATUS",
        "SHP",
        "PKTNO",
    ]:
        df_nns_data[col] = df_nns_data[col].astype(str).replace("nan", "")

    for col in ["MAX_CUR_FINAL_RATE_PD", "AVG_MPM_PD", "MPM_PD", "ORG_CTS", "CTS"]:
        df_nns_data[col] = pd.to_numeric(df_nns_data[col], errors="coerce").fillna(0)

    lot_code_map = lot_list.set_index("LotNo")["CODE"]
    orgcts_map = rough_detail.set_index("LotNo")["OrgCts"]
    labour_map = rough_detail.set_index("LotNo")["LABOUR"]
    expense_map = pd.Series(0, index=rough_detail.set_index("LotNo").index)
    month_map = rough_detail.set_index("LotNo")["SightMonth"]
    dtc_map = rough_detail.set_index("LotNo")["DTC_NONDTC"]
    lot_exists_map = rough_detail.set_index("LotNo").index.to_series()

    df_sales_processed = df_sales_processed.copy()
    df_sales_processed["Stone Id"] = df_sales_processed["Stone Id"].astype(str).replace("nan", "")
    sales_value_map = df_sales_processed.set_index("Stone Id")["Sales Value"]
    harsh_sales_value_map = df_sales_processed.set_index("Stone Id")["Harshbhai Sales"]
    lab_expense_map = df_sales_processed.set_index("Stone Id")["Lab Expense"]
    valid_hs_ids = df_sales_processed["Stone Id"].astype(str).replace("nan", "")

    df_current_getrate = df_current_getrate.copy()
    df_current_getrate["Lotno"] = df_current_getrate["Lotno"].astype(str).replace("nan", "")
    df_current_getrate["Main  Pktno"] = pd.to_numeric(df_current_getrate["Main  Pktno"], errors="coerce").fillna(0)

    def make_key(row: pd.Series) -> str:
        parts = [row["DISP_LOTNO"], row["MAIN_PKTNO"], row["TAG_NAME"]]
        return "#".join([str(x) for x in parts if x not in (None, "", "nan")])

    df_nns_data["Key"] = df_nns_data.apply(make_key, axis=1)

    sum_max_by_lot = df_nns_data.groupby("DISP_LOTNO")["MAX_CUR_FINAL_RATE_PD"].transform("sum")
    sum_avg_by_lot = df_nns_data.groupby("DISP_LOTNO")["AVG_MPM_PD"].transform("sum")
    df_nns_data["PRICE CHANGE"] = np.where(
        sum_avg_by_lot != 0, (sum_max_by_lot / sum_avg_by_lot) - 1, np.nan
    )

    df_nns_data["NEW CURRENT"] = np.where(
        df_nns_data["RATE_TYPE"] == "AvgReceived",
        df_nns_data["MAX_CUR_FINAL_RATE_PD"] * (1 + df_nns_data["PRICE CHANGE"]),
        df_nns_data["MAX_CUR_FINAL_RATE_PD"],
    )
    df_nns_data["NEW CURRENT"] = df_nns_data["NEW CURRENT"].where(
        np.isfinite(df_nns_data["NEW CURRENT"]), df_nns_data["MAX_CUR_FINAL_RATE_PD"]
    ).fillna(df_nns_data["MAX_CUR_FINAL_RATE_PD"])

    df_nns_data["COUNT"] = df_nns_data.groupby("Key")["Key"].transform("count")

    stone_id_str = df_nns_data["STONE_ID"].astype(str).replace("nan", "")
    sidtype_str = df_nns_data["SIDTYPE"].astype(str).replace("nan", "")

    sold_base = np.where(
        stone_id_str.eq(""),
        np.where(sidtype_str.eq("LabReceivedFinal"), df_nns_data["AVG_MPM_PD"], 0),
        np.nan,
    )
    df_nns_data["Sold"] = sold_base
    mask_nonblank_stone = ~stone_id_str.eq("")
    df_nns_data.loc[mask_nonblank_stone, "Sold"] = (
        stone_id_str[mask_nonblank_stone].map(sales_value_map).fillna(0).values
    )
    df_nns_data["Sold"] = pd.to_numeric(df_nns_data["Sold"], errors="coerce").fillna(0)

    df_nns_data["Sales+Current"] = np.where(
        df_nns_data["Sold"] == 0, df_nns_data["NEW CURRENT"], df_nns_data["Sold"]
    )

    df_nns_data["AVG_MPM_PD+By Product"] = np.where(
        df_nns_data["AVG_MPM_PD"] == 0, df_nns_data["MPM_PD"], df_nns_data["AVG_MPM_PD"]
    )

    sum_by_lot_apbp = df_nns_data.groupby("DISP_LOTNO")["AVG_MPM_PD+By Product"].transform("sum")
    df_nns_data["Running %"] = np.where(
        sum_by_lot_apbp != 0, df_nns_data["AVG_MPM_PD+By Product"] / sum_by_lot_apbp, 0
    )

    df_nns_data["Rough Cost"] = (
        df_nns_data["DISP_LOTNO"].map(lot_code_map).fillna(0)
        * df_nns_data["Running %"]
        * df_nns_data["DISP_LOTNO"].map(orgcts_map).fillna(0)
    )

    df_nns_data["Labour"] = (
        (df_nns_data["DISP_LOTNO"].map(labour_map).fillna(0) + df_nns_data["DISP_LOTNO"].map(expense_map).fillna(0))
        * df_nns_data["Running %"]
    )

    df_nns_data["Sales"] = np.where(
        df_nns_data["AVG_MPM_PD"] == 0,
        df_nns_data["MPM_PD"],
        df_nns_data["Sales+Current"],
    ) - df_nns_data["Labour"]

    df_nns_data["P & L"] = df_nns_data["Sales"] - df_nns_data["Rough Cost"]
    df_nns_data["P & L %"] = np.where(
        df_nns_data["Rough Cost"] != 0, df_nns_data["P & L"] / df_nns_data["Rough Cost"], 0
    )

    stone_status_str = df_nns_data["STONE_STATUS"].astype(str).replace("nan", "")
    mask_not_inprocess = sidtype_str.ne("InProcess")
    mask_not_byproducts = stone_status_str.ne("ByProducts")

    tmp_comp = pd.DataFrame(
        {
            "DISP_LOTNO": df_nns_data["DISP_LOTNO"],
            "num": np.where(mask_not_inprocess & mask_not_byproducts, df_nns_data["AVG_MPM_PD"], 0.0),
            "den": np.where(mask_not_byproducts, df_nns_data["AVG_MPM_PD"], 0.0),
        }
    )

    num_by_lot = tmp_comp.groupby("DISP_LOTNO")["num"].transform("sum")
    den_by_lot = tmp_comp.groupby("DISP_LOTNO")["den"].transform("sum")

    comp_raw = np.where(den_by_lot != 0, num_by_lot / den_by_lot, np.nan)
    df_nns_data["Comp%"] = pd.Series(comp_raw).fillna(1.0)

    df_nns_data["Lot Status"] = np.where(
        df_nns_data["Comp%"].notna(),
        np.where(df_nns_data["Comp%"] >= 0.9, "Complete", "Running"),
        "0%",
    )

    main_pktno_num = pd.to_numeric(df_nns_data["MAIN_PKTNO"], errors="coerce").fillna(0)
    df_nns_data["Inv Status"] = np.where(
        df_nns_data["Sold"] == 0,
        np.where((df_nns_data["Lot Status"] == "Complete") & (main_pktno_num == 0), "Sold", "Inv"),
        "Sold",
    )

    df_nns_data["Month"] = df_nns_data["DISP_LOTNO"].map(month_map)
    df_nns_data["DTC_NON DTC"] = df_nns_data["DISP_LOTNO"].map(dtc_map)

    included_lot = df_nns_data["DISP_LOTNO"].map(lot_exists_map)
    df_nns_data["Include Lots"] = included_lot.notna() & (included_lot.astype(str) != "")

    df_nns_data["Harshbhai Sales Status"] = np.where(
        stone_id_str == "", False, stone_id_str.isin(valid_hs_ids)
    )
    mask_hs = df_nns_data["Harshbhai Sales Status"]
    df_nns_data["Harshbhai Sales"] = 0.0
    df_nns_data.loc[mask_hs, "Harshbhai Sales"] = (
        stone_id_str[mask_hs].map(harsh_sales_value_map).fillna(0.0).values
    )
    df_nns_data["Lab Expense"] = 0.0
    df_nns_data.loc[mask_hs, "Lab Expense"] = (
        stone_id_str[mask_hs].map(lab_expense_map).fillna(0.0).values
    )

    df_nns_data["Rel"] = df_nns_data["AVG_MPM_PD+By Product"] - df_nns_data["Labour"]

    def make_pkt_key(row: pd.Series) -> str:
        parts = [row["DISP_LOTNO"], row["MAIN_PKTNO"]]
        return "#".join([str(x) for x in parts if x not in (None, "", "nan")])

    df_nns_data["PKT KEY"] = df_nns_data.apply(make_pkt_key, axis=1)

    pktno_str = df_nns_data["PKTNO"].astype(str).replace("nan", "")
    tag_name_str = df_nns_data["TAG_NAME"].astype(str).replace("nan", "")

    same_main_pkt = main_pktno_num == pd.to_numeric(df_nns_data["PKTNO"], errors="coerce").fillna(0)
    tag_not_a = tag_name_str.ne("A")

    def make_new_pktno(row: pd.Series) -> str:
        parts = [row["DISP_LOTNO"], row["PKTNO"]]
        return "#".join([str(x) for x in parts if x not in (None, "", "nan")])

    df_nns_data["New Pktno"] = np.where(
        same_main_pkt & tag_not_a, "", df_nns_data.apply(make_new_pktno, axis=1)
    )

    indicator = (main_pktno_num != 0).astype(int)
    dup_by_key = pd.DataFrame({"Key": df_nns_data["Key"], "ind": indicator}).groupby("Key")["ind"].transform("sum")
    df_nns_data["Duplicate"] = dup_by_key

    grp_lot_main = df_nns_data.groupby(["DISP_LOTNO", "MAIN_PKTNO"], dropna=False)
    sum_cts_by_lot_main = grp_lot_main["CTS"].transform("sum")
    max_orgcts_by_lot_main = grp_lot_main["ORG_CTS"].transform("max")
    df_nns_data["Polish Yield%"] = np.where(
        max_orgcts_by_lot_main != 0, sum_cts_by_lot_main / max_orgcts_by_lot_main, 0
    )

    shp_str = df_nns_data["SHP"].astype(str).replace("nan", "")
    is_r_emd_shape = shp_str.eq("R-EMD").astype(int)
    tmp_r_emd = pd.DataFrame({
        "DISP_LOTNO": df_nns_data["DISP_LOTNO"],
        "MAIN_PKTNO": df_nns_data["MAIN_PKTNO"],
        "ind": is_r_emd_shape,
    })
    r_emd_count_by_lot_main = tmp_r_emd.groupby(["DISP_LOTNO", "MAIN_PKTNO"], dropna=False)["ind"].transform("sum")
    df_nns_data["R-EMD PCS"] = r_emd_count_by_lot_main > 0

    return df_nns_data


# ---------------------------------------------------------------------------
# Final orchestration
# ---------------------------------------------------------------------------


def calculate_r_emd_loss_gain(
    df_current_get_rate: pd.DataFrame, df_nns_data: pd.DataFrame
) -> pd.DataFrame:
    lookup = df_nns_data[["DISP_LOTNO", "MAIN_PKTNO", "R-EMD PCS"]].copy()
    lookup["MAIN_PKTNO"] = pd.to_numeric(lookup["MAIN_PKTNO"], errors="coerce").astype(pd.Int64Dtype())

    nns_status = lookup.groupby(["DISP_LOTNO", "MAIN_PKTNO"]).any().reset_index()
    nns_status = nns_status.rename(columns={"R-EMD PCS": "NNS_HAS_R_EMD"})

    df_current_get_rate = df_current_get_rate.copy()
    df_current_get_rate["Lotno_for_merge"] = df_current_get_rate["Lotno"].astype(str)
    df_current_get_rate["Main_Pktno_Int_for_merge"] = df_current_get_rate["Main_Pktno_Int"].astype(pd.Int64Dtype())

    df_current_get_rate = df_current_get_rate.merge(
        nns_status,
        left_on=["Lotno_for_merge", "Main_Pktno_Int_for_merge"],
        right_on=["DISP_LOTNO", "MAIN_PKTNO"],
        how="left",
    )
    df_current_get_rate["NNS_HAS_R_EMD"] = df_current_get_rate["NNS_HAS_R_EMD"].fillna(False)

    df_current_get_rate["R-EMD LOSS PCS"] = (
        df_current_get_rate["R-EMD PCS"] & ~df_current_get_rate["NNS_HAS_R_EMD"]
    ).astype(int)
    df_current_get_rate["R-EMD GAIN PCS"] = (
        ~df_current_get_rate["R-EMD PCS"] & df_current_get_rate["NNS_HAS_R_EMD"]
    ).astype(int)

    return df_current_get_rate.drop(
        columns=["DISP_LOTNO", "MAIN_PKTNO", "NNS_HAS_R_EMD", "Lotno_for_merge", "Main_Pktno_Int_for_merge"],
        errors="ignore",
    )


def run_pipeline(sources: WorkbookSources) -> None:
    xls_main = pd.ExcelFile(sources.main)
    xls_lot_master = pd.ExcelFile(sources.lot_master)

    df_lot_list = process_lot_list(xls_main)
    df_multi_box = process_multi_box(xls_main)
    df_or_uncut = process_or_uncut(xls_main)
    df_direct_sales = process_direct_sales(xls_main)
    damage_value = load_damage(xls_main, xls_lot_master)
    df_first_signer_missing_data = load_simple_sheet(xls_main, "1st SIGNER MISSING DATA")
    df_sales_processed = process_harshbhai_sales(xls_main)
    df_result_get_rate = process_result_get_rate(xls_main)
    df_current_get_rate = process_current_get_rate(xls_main)
    df_nns_data = process_result_data(xls_main)
    df_rough_detail = process_rough_detail(xls_main)

    df_nns_data = calculate_nns_result(
        df_nns_data, df_lot_list, df_rough_detail, df_current_get_rate, df_sales_processed
    )
    df_current_get_rate = calculate_r_emd_loss_gain(df_current_get_rate, df_nns_data)

    with pd.ExcelWriter(sources.output, engine="xlsxwriter") as writer:
        df_lot_list.to_excel(writer, sheet_name="LOT LIST", index=False)
        df_multi_box.to_excel(writer, sheet_name="Multi Box", index=False)
        df_or_uncut.to_excel(writer, sheet_name="OR UNCUT", index=False)
        df_direct_sales.to_excel(writer, sheet_name="DIRECT SALES", index=False)
        damage_value.to_excel(writer, sheet_name="DAMAGE VALUE", index=False)
        df_first_signer_missing_data.to_excel(writer, sheet_name="1st SIGNER MISSING DATA", index=False)
        df_sales_processed.to_excel(writer, sheet_name="Harshbhai Sales", index=False)
        df_result_get_rate.to_excel(writer, sheet_name="Result Get Rate", index=False)
        df_current_get_rate.to_excel(writer, sheet_name="CURRENT GET RATE", index=False)
        df_rough_detail.to_excel(writer, sheet_name="Rough Detail", index=False)
        df_nns_data.to_excel(writer, sheet_name="NNS DATA", index=False)

    print(f"All processed DataFrames have been saved to '{sources.output}'.")


def parse_args(argv: Optional[Iterable[str]] = None) -> WorkbookSources:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--main", required=True, type=Path, help="Path to the main Excel workbook")
    parser.add_argument("--lot-master", required=True, type=Path, help="Path to the Lot Master workbook")
    parser.add_argument("--output", type=Path, default=Path("processed_data_summary.xlsx"), help="Output Excel file path")
    args = parser.parse_args(argv)
    return WorkbookSources(main=args.main, lot_master=args.lot_master, output=args.output)


def main(argv: Optional[Iterable[str]] = None) -> None:
    sources = parse_args(argv)
    run_pipeline(sources)


if __name__ == "__main__":
    main()
