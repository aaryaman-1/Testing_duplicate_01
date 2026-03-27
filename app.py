import streamlit as st
import pandas as pd
import io
import contextlib

from backend_logic import (
    load_excel_master_dataframe,
    extract_filtered_excel_inputs,
    find_duplicates_multi_new
)

st.set_page_config(
    page_title="Duplicates Identification Tool",
    layout="wide"
)

st.title("Duplicates Identification Tool")

# =========================================================
# CACHE EXCEL LOADING (1 DAY CACHE)
# =========================================================

@st.cache_data(ttl=86400)
def cached_load_excel(uploaded_file):
    """
    Cache Excel for 1 day.
    Prevents repeated heavy MBOM loading.
    """
    return load_excel_master_dataframe(uploaded_file)

# =========================================================
# HELPER
# =========================================================

def multiline_to_list(text):
    if not text:
        return []
    return [line.strip() for line in text.splitlines() if line.strip()]

def clean_output_text(output: str):
    """
    (Unused now — retained to avoid modifying structure)
    """
    duplicate_phrase = "the following combinations are forming duplicates"
    no_dup_line = "No duplicates are forming with the existing parts."

    if duplicate_phrase in output:
        output = output.replace(no_dup_line, "")
        return output.strip()

    if no_dup_line in output:
        return no_dup_line

    return output.strip()

# =========================================================
# MODE SELECTOR
# =========================================================

mode = st.radio(
    "Select Input Method",
    ["Manual User Input", "Excel File Extraction"],
    horizontal=True
)

# =========================================================
# MANUAL MODE (UPDATED OUTPUT ONLY)
# =========================================================

if mode == "Manual User Input":

    st.subheader("New / Modified Parts")

    col1, col2 = st.columns(2)

    with col1:
        new_product_numbers_text = st.text_area(
            "New/Modified Product Numbers (one per line)",
            height=200
        )

    with col2:
        new_ecdvs_text = st.text_area(
            "New/Modified Product ECDVs (one per line)",
            height=200
        )

    st.markdown("---")

    st.subheader("Existing Parts")

    col3, col4 = st.columns(2)

    with col3:
        other_product_numbers_text = st.text_area(
            "Existing Product Numbers (one per line)",
            height=200
        )

    with col4:
        other_ecdvs_text = st.text_area(
            "Existing Product ECDVs (one per line)",
            height=200
        )

    st.info(
        """
Manual Mode Notes:
- Date filtering must be done manually.
- Enter only same Code Function parts.
"""
    )

    if st.button("Check Duplicate"):

        new_product_numbers = multiline_to_list(new_product_numbers_text)
        new_ecdvs = multiline_to_list(new_ecdvs_text)

        other_product_numbers = multiline_to_list(other_product_numbers_text)
        other_ecdvs = multiline_to_list(other_ecdvs_text)

        if len(new_product_numbers) != len(new_ecdvs):
            st.error("Mismatch: New Product Numbers vs New ECDVs.")
            st.stop()

        if len(other_product_numbers) != len(other_ecdvs):
            st.error("Mismatch: Existing Product Numbers vs Existing ECDVs.")
            st.stop()

        # ✅ NEW STRUCTURED OUTPUT
        rows = find_duplicates_multi_new(
            new_ecdvs,
            other_ecdvs,
            new_product_numbers,
            other_product_numbers
        )

        if rows:
            df = pd.DataFrame(rows)

            st.subheader("Duplicate Table")
            st.dataframe(df, use_container_width=True)
            # Create an expander with the copyable text
            with st.expander("📋 Copy Table for Excel"):
                # Convert to Tab-Separated format (Excel loves tabs)
                tsv_data = df.to_csv(index=False, sep='\t')
                st.code(tsv_data, language="text")

            csv = df.to_csv(index=False).encode("utf-8")

            st.download_button(
                label="Download as CSV",
                data=csv,
                file_name="duplicates_output.csv",
                mime="text/csv"
            )
        else:
            st.info("No duplicates are forming with the existing parts.")

# =========================================================
# EXCEL MODE (UPDATED OUTPUT ONLY)
# =========================================================

elif mode == "Excel File Extraction":

    st.subheader("New / Modified Parts")

    col1, col2, col3, col4 = st.columns([2, 1, 4, 2])

    with col1:
        new_product_numbers_text = st.text_area(
            "New/Modified Product Numbers (one per line)",
            height=200
        )

    with col2:
        new_quantities_text = st.text_area(
            "Quantities",
            height=200
        )

    with col3:
        new_ecdvs_text = st.text_area(
            "New/Modified Product ECDVs (one per line)",
            height=200
        )

    with col4:
        new_dates_text = st.text_area(
            "New/Modified Product NFC Dates (YYYY-MM-DD)",
            height=200
        )
    
    # New Product Name text area positioned outside columns to match Code Function width
    new_product_names_text = st.text_area(
        "New/Modified Product Names (one per line)",
        height=200
    )

    st.markdown("---")

    # ✅ ADDED: Cancelled Parts UI
    st.subheader("Cancelled Parts")
    c_col1, c_col2, c_col3, c_col4 = st.columns([2, 1, 3, 3])

    with c_col1:
        cancel_product_numbers_text = st.text_area("Cancel Product Numbers", height=200)

    with c_col2:
        cancel_quantities_text = st.text_area("Cancel Quantities", height=200)

    with c_col3:
        cancel_product_names_text = st.text_area("Cancel Product Names", height=200)

    with c_col4:
        cancel_ecdvs_text = st.text_area("Cancel ECDVs", height=200)

    st.markdown("---")

    code_function = st.text_input("Code Function")

    uploaded_file = st.file_uploader(
        "Upload MBOM Extraction Excel",
        type=["xlsx"]
    )

    if st.button("Check Duplicate"):

        if uploaded_file is None:
            st.error("Upload Excel file first.")
            st.stop()

        if not code_function:
            st.error("Enter Code Function.")
            st.stop()

        # 1. Read base text inputs
        new_product_numbers = multiline_to_list(new_product_numbers_text)
        new_ecdvs = multiline_to_list(new_ecdvs_text)
        new_dates = multiline_to_list(new_dates_text)
        
        raw_quantities = multiline_to_list(new_quantities_text)
        raw_product_names = multiline_to_list(new_product_names_text)

        is_major = (len(code_function.strip()) == 8)

        # 2. Smart Quantity Handling (Optional for Major)
        if is_major and not raw_quantities:
            new_quantities = [None] * len(new_product_numbers)
        else:
            new_quantities = [float(q) for q in raw_quantities]

        # 3. Smart Product Name Handling (Optional for Major)
        if is_major and not raw_product_names:
            new_product_names = [""] * len(new_product_numbers)
        else:
            new_product_names = raw_product_names

        # 4. Standard Length Validations
        if len(new_product_numbers) != len(new_ecdvs):
            st.error("Mismatch: New Product Numbers vs ECDVs.")
            st.stop()

        if len(new_product_numbers) != len(new_dates):
            st.error("Mismatch: New Product Numbers vs NFC Dates.")
            st.stop()

        if len(new_product_numbers) != len(new_quantities):
            st.error("Mismatch: Product Numbers vs Quantities.")
            st.stop()
            
        if len(new_product_numbers) != len(new_product_names):
            st.error("Mismatch: Product Numbers vs Product Names.")
            st.stop()

        # 5. Read Cancelled Text Inputs
        cancel_product_numbers = multiline_to_list(cancel_product_numbers_text)
        raw_cancel_quantities = multiline_to_list(cancel_quantities_text)
        cancel_product_names = multiline_to_list(cancel_product_names_text)
        cancel_ecdvs = multiline_to_list(cancel_ecdvs_text)

        if cancel_product_numbers:
            if len(cancel_product_numbers) != len(raw_cancel_quantities):
                st.error("Mismatch: Cancel Numbers vs Cancel Quantities.")
                st.stop()
            if len(cancel_product_numbers) != len(cancel_product_names):
                st.error("Mismatch: Cancel Numbers vs Cancel Names.")
                st.stop()
            if len(cancel_product_numbers) != len(cancel_ecdvs):
                st.error("Mismatch: Cancel Numbers vs Cancel ECDVs.")
                st.stop()
            cancel_quantities = [float(q) for q in raw_cancel_quantities]
        else:
            cancel_quantities = []


        df_master = cached_load_excel(uploaded_file)

        all_rows = []

        # NEW vs EXISTING
        for i in range(len(new_product_numbers)):

            other_product_numbers, other_product_names, other_ecdvs, other_quantities = extract_filtered_excel_inputs(
                df_master=df_master,
                code_function=code_function,
                new_product_NFCdate=new_dates[i],
                new_quantity=new_quantities[i],
                cancel_product_numbers=cancel_product_numbers,
                cancel_quantities=cancel_quantities,
                cancel_ecdvs=cancel_ecdvs,
                cancel_product_names=cancel_product_names
            )

            rows = find_duplicates_multi_new(
                [new_ecdvs[i]],
                other_ecdvs,
                [new_product_numbers[i]],
                other_product_numbers,
                [new_quantities[i]],
                other_quantities,
                [new_product_names[i]],
                other_product_names,
                code_function
            )

            all_rows.extend(rows)

        # NEW vs NEW
        rows = find_duplicates_multi_new(
            new_ecdvs,
            [],
            new_product_numbers,
            [],
            new_quantities,
            [],
            new_product_names,
            [],
            code_function
        )

        all_rows.extend(rows)

        if all_rows:
            df = pd.DataFrame(all_rows)

            st.subheader("Duplicate Table")

            st.dataframe(df, use_container_width=True)

            # Create an expander with the copyable text
            with st.expander("📋 Copy Table for Excel"):
                # Convert to Tab-Separated format (Excel loves tabs)
                tsv_data = df.to_csv(index=False, sep='\t')
                st.code(tsv_data, language="text")

            csv = df.to_csv(index=False).encode("utf-8")

            st.download_button(
                label="Download as CSV",
                data=csv,
                file_name="duplicates_output.csv",
                mime="text/csv"
            )
        else:
            st.info("No duplicates are forming with the existing parts.")
