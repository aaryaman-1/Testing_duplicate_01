import pandas as pd
import re
from datetime import datetime
import io
import contextlib
from zoneinfo import ZoneInfo

# =========================================================
# NEW HELPER (CM + FAMILY EXTRACTION)
# =========================================================

def extract_cm_family(ecdv: str):
    """
    Extract:
    CM      → characters before first dot
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

    # ==========================================
    # FIX: Handle "Tous" type (Universal) ECDV
    # If there is no remainder after the CM/Family,
    # it is a wildcard matching all configurations.
    # ==========================================
    if not remainder:
        return pd.DataFrame([{}])

    if "<" in remainder:
        common_str, body = remainder.split("<", 1)
        common_parts = re.findall(
            r"\([A-Z0-9]+[A-Z0-9]{2}\)|[A-Z0-9]+[A-Z0-9]{2}",
            common_str
        )
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

    # ==========================================
    # FIX: Bulletproof Fallback
    # Prevents crashes on anomalous string parses
    # ==========================================
    if not parsed_rows:
        return pd.DataFrame([{}])

    for row in parsed_rows:
        for part in common_parts:
            is_exception = False
            
            if part.startswith("("):
                is_exception = True
                part = part[1:-1]
                
            col = part[:-2]
            val = part[-2:]
            
            if is_exception:
                val = f"!{val}"
            
            # Combine logic
            if col in row:
                existing = row[col]
                if not isinstance(existing, list):
                    existing = [existing]
                existing.append(val)
                row[col] = existing
            else:
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

def window_overlap(row1_windows, row2_windows, nfc_date=None):

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

    french_date_obj = datetime.now(ZoneInfo("Europe/Paris"))
    idx_now = get_window_from_date(french_date_obj.strftime("%Y-%m-%d"))

    # logic to find the START of the current quarter in France
    q_letter, _, q_year = quarter_from_date(french_date_obj)
    month_map_q = {"A": 1, "B": 4, "C": 7, "D": 10}
    current_quarter_start = datetime(q_year, month_map_q[q_letter], 1)

    start_idx = idx_now - 10
    end_idx = idx_now + 6

    windows_list = []
    quarters_list = []

    for idx in range(start_idx, end_idx + 1):
        windows_list.append(sequence_element_from_index(idx))
        quarters_list.append(quarter_from_index(idx))

    types = ["closing"] * 9 + ["opening"] * 8

    df_ref = pd.DataFrame({
        "Window": windows_list,
        "type": types,
        "quarter": quarters_list
    })

    # ---------------------------------------
    # Determine Fallback Start Date (NFC or Now)
    # ---------------------------------------
    if nfc_date is not None:
        if isinstance(nfc_date, str):
            d_nfc = pd.to_datetime(nfc_date)
        else:
            d_nfc = nfc_date
        
        m_nfc, y_nfc = d_nfc.month, d_nfc.year
        if m_nfc <= 3: q_start_month = 1
        elif m_nfc <= 6: q_start_month = 4
        elif m_nfc <= 9: q_start_month = 7
        else: q_start_month = 10
        
        fallback_start_date = datetime(y_nfc, q_start_month, 1)
    else:
        fallback_start_date = datetime(french_date_obj.year, french_date_obj.month, french_date_obj.day)

    # ---------------------------------------
    # Step 3: Convert windows → date range
    # ---------------------------------------

    def get_date_range(window_list):

        if not window_list:
            return (fallback_start_date, datetime.max)

        start_date = None
        end_date = None

        for w in window_list:
            match = df_ref[df_ref["Window"] == w]
            if match.empty:
                continue

            w_type = match.iloc[0]["type"]
            q = match.iloc[0]["quarter"]
            q_let = q[0]
            year = int(q[1:])
            month_map = {"A": 1, "B": 4, "C": 7, "D": 10}

            if w_type == "opening":
                start_date = datetime(year, month_map[q_let], 2)
            else:
                end_date = datetime(year + 2, month_map[q_let], 1)

        if start_date is None: start_date = datetime.min
        if end_date is None: end_date = datetime.max
        return start_date, end_date

    r1_start, r1_end = get_date_range(row1_w)
    r2_start, r2_end = get_date_range(row2_w)

    if r1_start is None or r2_start is None:
        return False

    # ---------------------------------------
    # Step 4: OVERLAP DURATION CHECK
    # ---------------------------------------
    overlap_start = max(r1_start, r2_start)
    overlap_end = min(r1_end, r2_end)

    is_overlapping = overlap_start <= overlap_end
    is_not_historical = overlap_end >= current_quarter_start

    return is_overlapping and is_not_historical

def rows_are_duplicate(row1, row2, columns, nfc_date=None):

    # -------------------------------------------------
    # WINDOW COLUMN LOGIC (CASE 1 + CASE 2)
    # -------------------------------------------------

    window_cols = {"W4", "R7", "R0", "R8", "V7", "V8", "V0", "V9"}
    present_window_cols = [c for c in columns if c in window_cols]

    if present_window_cols:
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

            row1_main = row1[main_columns]
            row2_main = row2[main_columns]

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

            row1_windows = row1[window_columns]
            row2_windows = row2[window_columns]

            if window_overlap(row1_windows, row2_windows, nfc_date):
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
                if parts:
                    parts[-1] = parts[-1] + exclusion_text
                else:
                    parts.append(exclusion_text)
            else:
                parts.append(f"{col}{v}")

    return ".".join(parts) if parts else "ALL"

# =========================================================
# DUPLICATE ENGINE (UPDATED WITH DYNAMIC FORMATTING)
# =========================================================

def find_duplicates_one_to_many(
        new_ecdv,
        other_ecdvs,
        new_product_number=None,
        other_product_numbers=None,
        new_quantity=None,
        other_quantities=None,
        new_product_name=None,
        other_product_names=None,
        code_function=None,
        new_nfc_date=None
):

    result_rows = []
    
    # Establish whether this is a Major or Minor Code Function
    is_major = True
    if code_function is not None:
        is_major = (len(str(code_function).strip()) == 8)

    for idx, ecdv in enumerate(other_ecdvs):

        if is_major:
            if new_product_number and other_product_numbers:
                # Updated per your instructions: only verify product number
                if new_product_number == other_product_numbers[idx]:
                    continue
        
        # ----------------------------------------------------------
        # NEW LOGIC: MINOR CODE FUNCTION CHECK
        # ----------------------------------------------------------
        if not is_major:
            # Rule 2: Quantity variables must be equal
            q1 = new_quantity
            q2 = other_quantities[idx] if other_quantities else None
            if q1 != q2:
                continue
                
            # Rule 3: Product names must match
            n1 = str(new_product_name).strip() if new_product_name else None
            n2 = str(other_product_names[idx]).strip() if other_product_names else None
            if n1 != n2:
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

                if rows_are_duplicate(row1, row2, columns, nfc_date=new_nfc_date):
                    combo1 = row_to_combination_string(row1)
                    combo2 = row_to_combination_string(row2)
                    duplicate_pairs.append(f"{combo1} and {combo2}")

        if duplicate_pairs:

            unique_pairs = list(dict.fromkeys(duplicate_pairs))

            part1 = f"{new_product_number}" if new_product_number else "part 1"
            part2 = f"{other_product_numbers[idx]}" if other_product_numbers else f"part {idx+2}"

            # ✅ DYNAMIC OUTPUT FORMATTING BASED ON EXCEL REQUIREMENTS
            if is_major:
                result_rows.append({
                    "duplicate ref 1": part1,
                    "quantity 1": new_quantity,
                    "duplicate ref 2": part2,
                    "quantity 2": other_quantities[idx] if other_quantities else None,
                    "combinations forming duplicate": ", ".join(unique_pairs)
                })
            else:
                result_rows.append({
                    "duplicate ref 1": part1,
                    "name1": new_product_name,
                    "quantity 1": new_quantity,
                    "duplicate ref 2": part2,
                    "name2": other_product_names[idx] if other_product_names else None,
                    "quantity 2": other_quantities[idx] if other_quantities else None,
                    "Duplicate Combinations": ", ".join(unique_pairs)
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
        "Designation produit",     
        "02 Code fonction lien vehicule",
        "Coefficient de montage",
        "ECDV",
        "Date application OEV debut",
        "Date application OEV fin"
    ]

    df_master = df_master[required_columns].copy()

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
    new_quantity,
    cancel_product_numbers=None, 
    cancel_quantities=None,      
    cancel_ecdvs=None,           
    cancel_product_names=None    
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

    other_product_numbers = []
    other_product_names = []  
    other_ecdvs = []
    other_quantities = []

    # Safe fallback if cancellation arrays are empty
    if not cancel_product_numbers:
        cancel_product_numbers = []
        cancel_quantities = []
        cancel_ecdvs = []
        cancel_product_names = []

    for _, row in df_filtered.iterrows():

        product = str(row["05 Numero produit"]).strip()
        name = str(row["Designation produit"]).strip() if pd.notna(row["Designation produit"]) else ""  
        ecdv = normalize_excel_ecdv_format(row["ECDV"])
        qty = row["Coefficient de montage"]

        if product and ecdv:
            
            # ✅ ADDED: Explicit Cancellation Check
            is_cancelled = False
            for i in range(len(cancel_product_numbers)):
                if (product == str(cancel_product_numbers[i]).strip() and
                    name == str(cancel_product_names[i]).strip() and
                    qty == cancel_quantities[i] and
                    ecdv == str(cancel_ecdvs[i]).strip()):
                    is_cancelled = True
                    break
            
            # Skip this row if it completely matched a cancelled entry
            if is_cancelled:
                continue

            other_product_numbers.append(product)
            other_product_names.append(name)
            other_ecdvs.append(ecdv)
            other_quantities.append(qty)

    return other_product_numbers, other_product_names, other_ecdvs, other_quantities

# =========================================================
# Multi-New Wrapper Function
# =========================================================

def find_duplicates_multi_new(
        new_ecdvs,
        other_ecdvs,
        new_product_numbers,
        other_product_numbers,
        new_quantities=None,
        other_quantities=None,
        new_product_names=None,      
        other_product_names=None,    
        code_function=None,          
        new_nfc_dates=None           
):

    if new_quantities is None: new_quantities = [None] * len(new_ecdvs)
    if other_quantities is None: other_quantities = [None] * len(other_ecdvs)
    
    if new_product_names is None: new_product_names = [None] * len(new_ecdvs)
    if other_product_names is None: other_product_names = [None] * len(other_ecdvs)
    
    if new_nfc_dates is None: new_nfc_dates = [None] * len(new_ecdvs)

    all_rows = []

    # ✅ CHANGED: Removed auto-filtering. Identical existing numbers are passed directly in.
    for i in range(len(new_ecdvs)):
        rows = find_duplicates_one_to_many(
            new_ecdvs[i],
            other_ecdvs,  
            new_product_numbers[i],
            other_product_numbers, 
            new_quantities[i],
            other_quantities,
            new_product_names[i],
            other_product_names,
            code_function,
            new_nfc_dates[i]
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
                [new_quantities[j]],
                new_product_names[i],
                [new_product_names[j]],
                code_function,
                new_nfc_dates[i]
            )
            all_rows.extend(rows)

    return all_rows
