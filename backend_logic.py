import pandas as pd
import re
from datetime import datetime
import io
import contextlib

# =========================================================
# NEW HELPER (CM + FAMILY EXTRACTION)
# =========================================================

def extract_cm_family(ecdv: str):
    """
    Extract:
    CM     → characters before first dot
    Family → first 4 characters after first dot
    """

    if not isinstance(ecdv, str):
        return None, None

    match = re.match(r'^([^.]+)\.([A-Za-z0-9]{4})', ecdv.strip())

    if not match:
        return None, None

    cm = match.group(1)
    family = match.group(2)

    return cm, family


# =========================================================
# ECDV INVERSE LOGIC
# =========================================================

def inverse_generate_ecdv(ecdv_string: str) -> pd.DataFrame:

    if not isinstance(ecdv_string, str):
        raise TypeError("ECDV input must be a string.")

    ecdv_string = ecdv_string.strip()

    if not ecdv_string:
        raise ValueError("Empty ECDV string.")

    if ecdv_string == "No combinations for this product line":
        raise ValueError("Cannot inverse: No combinations case.")

    if not ecdv_string.endswith("*"):
        raise ValueError("Invalid ECDV format (missing '*').")

    ecdv_string = ecdv_string[:-1]

    match = re.match(r'^([^.]+)\.([A-Za-z0-9]+)(.*)$', ecdv_string)

    if not match:
        raise ValueError("Invalid ECDV structure.")

    CM = match.group(1)
    Family = match.group(2)
    remainder = match.group(3)

    if remainder.startswith("."):
        remainder = remainder[1:]

    if "<" in remainder:
        common_str, body = remainder.split("<", 1)
        common_parts = [p for p in common_str.split(".") if p]
    else:
        common_parts = []
        body = remainder

    combinations = body.split("/") if body else []
    parsed_rows = []

    for combo in combinations:

        combo = combo.strip()
        if not combo:
            continue

        row_dict = {}

        tokens = re.findall(
            r"\([A-Z0-9]+[A-Z0-9]{2}\)|[A-Z0-9]+[A-Z0-9]{2}",
            combo
        )

        for token in tokens:

            is_exception = False

            if token.startswith("("):
                is_exception = True
                token = token[1:-1]

            col = token[:-2]
            val = token[-2:]

            if is_exception:
                val = f"!{val}"

            if col in row_dict:

                existing = row_dict[col]

                if not isinstance(existing, list):
                    existing = [existing]

                if any(not v.startswith("!") for v in existing):
                    raise ValueError(
                        f"Invalid ECDV: mixed inclusion/exclusion for column {col}"
                    )

                if not val.startswith("!"):
                    raise ValueError(
                        f"Invalid ECDV: mixed inclusion/exclusion for column {col}"
                    )

                existing.append(val)
                row_dict[col] = existing

            else:
                row_dict[col] = val

        parsed_rows.append(row_dict)

    if not parsed_rows:
        raise ValueError("No valid combinations parsed.")

    for row in parsed_rows:
        for part in common_parts:
            col = part[:-2]
            val = part[-2:]
            row[col] = val

    all_columns = sorted({col for row in parsed_rows for col in row.keys()})

    final_rows = []
    for row in parsed_rows:
        formatted = {}
        for col in all_columns:
            formatted[col] = row.get(col, [])
        final_rows.append(formatted)

    return pd.DataFrame(final_rows)


# =========================================================
# PREPROCESS FOR COMPARISON
# =========================================================

def preprocess_ecdv_for_comparison(ecdv1, ecdv2):

    df1 = inverse_generate_ecdv(ecdv1)
    df2 = inverse_generate_ecdv(ecdv2)

    all_columns = sorted(set(df1.columns).union(set(df2.columns)))

    for col in all_columns:
        if col not in df1.columns:
            df1[col] = [[] for _ in range(len(df1))]

    for col in all_columns:
        if col not in df2.columns:
            df2[col] = [[] for _ in range(len(df2))]

    df1 = df1[all_columns]
    df2 = df2[all_columns]

    return df1, df2


# =========================================================
# DUPLICATE CORE LOGIC
# =========================================================

def normalize_cell(val):

    if val == []:
        return []

    if isinstance(val, list):
        return [str(v) for v in val]

    return [str(val)]


def is_exclusion(v):
    return v.startswith("!")


def is_inclusion(v):
    return not v.startswith("!")

def window_overlap(row1_windows, row2_windows):

    from datetime import datetime
    from zoneinfo import ZoneInfo

    # ---------------------------------------
    # Step 1: Extract window elements
    # ---------------------------------------

    def extract_windows(row):
        windows = []
        for col, val in row.items():
            vals = normalize_cell(val)
            if vals:
                windows.append(f"{col}{vals[0]}")
        return windows

    row1_w = extract_windows(row1_windows)
    row2_w = extract_windows(row2_windows)

    # ---------------------------------------
    # Step 2: Generate reference dataframe
    # (UNCHANGED logic reused)
    # ---------------------------------------

    prefixes = ["W4", "R7", "R0", "R8", "V7", "V8", "V0", "V9"]
    half = 4
    block_size = 8

    def quarter_from_date(date):
        m, y = date.month, date.year
        if m <= 3: return "A", 0, y
        elif m <= 6: return "B", 1, y
        elif m <= 9: return "C", 2, y
        else: return "D", 3, y

    def sequence_element_from_index(idx):
        block = idx // block_size
        pos = idx % block_size
        prefix = prefixes[pos]
        value = 10 + block if pos < half else 9 + block
        return f"{prefix}{value:02d}"

    def get_window_from_date(date_string):
        date = datetime.strptime(date_string, "%Y-%m-%d")
        q, q_index, year = quarter_from_date(date)
        ref_year, ref_q = 2020, 0
        idx = (year - ref_year) * 4 + (q_index - ref_q)
        return idx

    def quarter_from_index(idx):
        quarters = ["A", "B", "C", "D"]
        year = 2020 + (idx // 4)
        return f"{quarters[idx % 4]}{year}"

    french_date = datetime.now(ZoneInfo("Europe/Paris")).date()
    idx_now = get_window_from_date(french_date.strftime("%Y-%m-%d"))

    start_idx = idx_now - 10
    end_idx = idx_now + 6

    windows = []
    quarters = []

    for idx in range(start_idx, end_idx + 1):
        windows.append(sequence_element_from_index(idx))
        quarters.append(quarter_from_index(idx))

    types = ["closing"] * 9 + ["opening"] * 8

    df = pd.DataFrame({
        "Window": windows,
        "type": types,
        "quarter": quarters
    })

    # ---------------------------------------
    # Step 3: Convert windows → date range
    # ---------------------------------------

    def get_date_range(window_list):

        if not window_list:
            return (
                french_date, 
                datetime.max
            )

        start_date = None
        end_date = None

        for w in window_list:

            match = df[df["Window"] == w]

            if match.empty:
                continue

            w_type = match.iloc[0]["type"]
            q = match.iloc[0]["quarter"]

            q_letter = q[0]
            year = int(q[1:])

            # Opening → start date
            if w_type == "opening":

                month_map = {"A": 1, "B": 4, "C": 7, "D": 10}
                start_date = datetime(year, month_map[q_letter], 2)

            # Closing → end date (+2 years)
            else:

                month_map = {"A": 1, "B": 4, "C": 7, "D": 10}
                end_date = datetime(year + 2, month_map[q_letter], 1)

        if start_date is None:
            start_date = datetime.min

        if end_date is None:
            end_date = datetime.max

        if start_date > end_date:
            raise ValueError("Invalid date range: window start_date is greater than end_date")

        return start_date, end_date

    r1_start, r1_end = get_date_range(row1_w)
    r2_start, r2_end = get_date_range(row2_w)

    if r1_start is None or r2_start is None:
        return False

    # ---------------------------------------
    # Step 4: Overlap check (inclusive)
    # ---------------------------------------

    return (r1_start <= r2_end) and (r2_start <= r1_end)

def rows_are_duplicate(row1, row2, columns):

    # -------------------------------------------------
    # WINDOW COLUMN LOGIC (CASE 1 + CASE 2)
    # -------------------------------------------------

    window_cols = {"W4", "R7", "R0", "R8", "V7", "V8", "V0", "V9"}

    present_window_cols = [c for c in columns if c in window_cols]

    if present_window_cols:

        # Check if any inclusion value exists in window columns
        window_has_values = False

        for col in present_window_cols:

            vals1 = normalize_cell(row1[col])
            vals2 = normalize_cell(row2[col])

            if vals1 or vals2:
                window_has_values = True
                break

        # -------------------------------------------------
        # CASE 2 → Window columns contain values
        # -------------------------------------------------
        if window_has_values:

            main_columns = [c for c in columns if c not in window_cols]
            window_columns = present_window_cols

            # Create reduced rows (without window columns)
            row1_main = row1[main_columns]
            row2_main = row2[main_columns]

            # Run original logic on rows without window columns
            for col in main_columns:

                vals1 = normalize_cell(row1_main[col])
                vals2 = normalize_cell(row2_main[col])

                if not vals1 or not vals2:
                    continue

                if (
                    all(is_inclusion(v) for v in vals1)
                    and all(is_inclusion(v) for v in vals2)
                ):
                    if vals1[0] != vals2[0]:
                        return False

            for col in main_columns:

                vals1 = normalize_cell(row1_main[col])
                vals2 = normalize_cell(row2_main[col])

                if not vals1 or not vals2:
                    continue

                if any(is_exclusion(v) for v in vals1) and all(is_inclusion(v) for v in vals2):
                    incl = vals2[0]
                    if f"!{incl}" in vals1:
                        return False

                if any(is_exclusion(v) for v in vals2) and all(is_inclusion(v) for v in vals1):
                    incl = vals1[0]
                    if f"!{incl}" in vals2:
                        return False

            # If we reached here → main rows ARE duplicates
            # Now check window overlap (to be implemented later)

            row1_windows = row1[window_columns]
            row2_windows = row2[window_columns]

            if window_overlap(row1_windows, row2_windows):
                return True
            else:
                return False

    # -------------------------------------------------
    # ORIGINAL LOGIC (UNCHANGED)
    # -------------------------------------------------

    for col in columns:

        vals1 = normalize_cell(row1[col])
        vals2 = normalize_cell(row2[col])

        if not vals1 or not vals2:
            continue

        if (
            all(is_inclusion(v) for v in vals1)
            and all(is_inclusion(v) for v in vals2)
        ):
            if vals1[0] != vals2[0]:
                return False

    for col in columns:

        vals1 = normalize_cell(row1[col])
        vals2 = normalize_cell(row2[col])

        if not vals1 or not vals2:
            continue

        if any(is_exclusion(v) for v in vals1) and all(is_inclusion(v) for v in vals2):
            incl = vals2[0]
            if f"!{incl}" in vals1:
                return False

        if any(is_exclusion(v) for v in vals2) and all(is_inclusion(v) for v in vals1):
            incl = vals1[0]
            if f"!{incl}" in vals2:
                return False

    return True


def row_to_combination_string(row):

    parts = []

    for col, val in row.items():

        if val == []:
            continue

        if not isinstance(val, list):
            val = [val]

        for v in val:
            v = str(v)

            # -----------------------------------
            # EXCLUSION VALUE → attach to previous
            # -----------------------------------
            if v.startswith("!"):

                exclusion_text = f"({col}{v[1:]})"

                # Attach to previous token if exists
                if parts:
                    parts[-1] = parts[-1] + exclusion_text
                else:
                    parts.append(exclusion_text)

            else:
                parts.append(f"{col}{v}")

    return ".".join(parts) if parts else "ALL"


# =========================================================
# DUPLICATE ENGINE (UPDATED WITH CM/FAMILY RULE)
# =========================================================

def find_duplicates_one_to_many(
        new_ecdv,
        other_ecdvs,
        new_product_number=None,
        other_product_numbers=None,
        new_quantity=None,
        other_quantities=None
):

    result_rows = []

    for idx, ecdv in enumerate(other_ecdvs):

        if new_product_number and other_product_numbers:
            if new_product_number == other_product_numbers[idx]:
                continue

        new_cm, new_family = extract_cm_family(new_ecdv)
        other_cm, other_family = extract_cm_family(ecdv)

        if (new_cm != other_cm) or (new_family != other_family):
            continue

        df1, df2 = preprocess_ecdv_for_comparison(new_ecdv, ecdv)
        columns = df1.columns
        duplicate_pairs = []

        for _, row1 in df1.iterrows():
            for _, row2 in df2.iterrows():

                if rows_are_duplicate(row1, row2, columns):
                    combo1 = row_to_combination_string(row1)
                    combo2 = row_to_combination_string(row2)
                    duplicate_pairs.append(f"{combo1} and {combo2}")

        if duplicate_pairs:

            unique_pairs = list(dict.fromkeys(duplicate_pairs))

            part1 = f"ref. {new_product_number}" if new_product_number else "part 1"
            part2 = f"ref. {other_product_numbers[idx]}" if other_product_numbers else f"part {idx+2}"

            result_rows.append({
                "duplicate ref 1": part1,
                "quantity 1": new_quantity,
                "duplicate ref 2": part2,
                "quantity 2": other_quantities[idx] if other_quantities else None,
                "combinations forming duplicate": ", ".join(unique_pairs)
            })

    return result_rows


# =========================================================
# EXCEL NORMALIZATION
# =========================================================

def normalize_excel_ecdv_format(ecdv: str):

    if not isinstance(ecdv, str):
        return ecdv

    ecdv = ecdv.strip()

    if not ecdv:
        return ecdv

    ecdv = re.sub(
        r'(?<=[\./<\(\)])(?:B0|D|F)(?=[A-Z0-9])',
        '',
        ecdv
    )

    return ecdv


# =========================================================
# EXCEL LOADER
# =========================================================

def load_excel_master_dataframe(file_path):

    df_master = pd.read_excel(
        file_path,
        header=1,
        dtype=str
    )

    required_columns = [
        "05 Numero produit",
        "02 Code fonction lien vehicule",
        "Coefficient de montage",
        "ECDV",
        "Date application OEV debut",
        "Date application OEV fin"
    ]

    df_master = df_master[required_columns].copy()

    # ✅ CHANGE: convert Coefficient to numeric
    df_master["Coefficient de montage"] = pd.to_numeric(
        df_master["Coefficient de montage"],
        errors="coerce"
    )

    df_master["Date application OEV debut"] = pd.to_datetime(
        df_master["Date application OEV debut"],
        errors="coerce",
        dayfirst=True,
        format="mixed"
    )

    df_master["Date application OEV fin"] = pd.to_datetime(
        df_master["Date application OEV fin"],
        errors="coerce",
        dayfirst=True,
        format="mixed"
    )

    OPEN_END_DATE = pd.Timestamp.max.normalize()

    df_master["Date application OEV fin"] = df_master[
        "Date application OEV fin"
    ].fillna(OPEN_END_DATE)

    return df_master


# =========================================================
# EXCEL FILTER ENGINE
# =========================================================

def extract_filtered_excel_inputs(
    df_master,
    code_function,
    new_product_NFCdate,
    new_quantity   # ✅ CHANGE: new parameter
):

    date_value = pd.to_datetime(new_product_NFCdate)

    df_filtered = df_master.copy()

    df_filtered = df_filtered[
        df_filtered["02 Code fonction lien vehicule"] == code_function
    ]

    df_filtered = df_filtered[
        (df_filtered["Date application OEV debut"] <= date_value) &
        (df_filtered["Date application OEV fin"] > date_value)
    ]

    df_filtered = df_filtered[
        df_filtered["Date application OEV debut"] != df_filtered["Date application OEV fin"]
    ]

    # ✅ CHANGE: filter by quantity
    df_filtered = df_filtered[
        df_filtered["Coefficient de montage"] == new_quantity
    ]

    other_product_numbers = []
    other_ecdvs = []
    other_quantities = []

    for _, row in df_filtered.iterrows():

        product = str(row["05 Numero produit"]).strip()
        ecdv = normalize_excel_ecdv_format(row["ECDV"])
        qty = row["Coefficient de montage"]

        if product and ecdv:
            other_product_numbers.append(product)
            other_ecdvs.append(ecdv)
            other_quantities.append(qty)

    return other_product_numbers, other_ecdvs, other_quantities
# =========================================================
# One new wrapper function
# =========================================================

def find_duplicates_multi_new(
        new_ecdvs,
        other_ecdvs,
        new_product_numbers,
        other_product_numbers,
        new_quantities=None,
        other_quantities=None
):

    if new_quantities is None:
        new_quantities = [None] * len(new_ecdvs)

    if other_quantities is None:
        other_quantities = [None] * len(other_ecdvs)

    all_rows = []

    # NEW vs EXISTING
    filtered_existing = [
        (pn, ev, qty)
        for pn, ev, qty in zip(other_product_numbers, other_ecdvs, other_quantities)
        if pn not in set(new_product_numbers)
    ]

    if filtered_existing:
        f_pn, f_ev, f_qty = zip(*filtered_existing)
    else:
        f_pn, f_ev, f_qty = [], [], []

    for i in range(len(new_ecdvs)):
        rows = find_duplicates_one_to_many(
            new_ecdvs[i],
            list(f_ev),
            new_product_numbers[i],
            list(f_pn),
            new_quantities[i],
            list(f_qty)
        )
        all_rows.extend(rows)

    # NEW vs NEW
    for i in range(len(new_ecdvs)):
        for j in range(i + 1, len(new_ecdvs)):
            rows = find_duplicates_one_to_many(
                new_ecdvs[i],
                [new_ecdvs[j]],
                new_product_numbers[i],
                [new_product_numbers[j]],
                new_quantities[i],
                [new_quantities[j]]
            )
            all_rows.extend(rows)

    return all_rows
