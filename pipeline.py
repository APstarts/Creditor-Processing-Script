import pandas as pd
import os
import gc
from rapidfuzz import fuzz, process

#Regex patterns to extract PO and GRN from the narration of the cleaned ledger Report that we get out of using the formatting function.
regex_patterns = {
    "PO": r"(PO\/\d+-\d+\/\d+)",
    "GRN": r"(?i)grn\s*(?:si)?\s*\.?\s*no\s*\.?\s*\-?\s*\:?\s*(.*?)(?=\s(?:dt|dated))",
}

#converting the files into parquet format for fastest processing
def convert_to_partquet(ledger_path:str, outPath:str):
    """Requires mentioning the folder which contains the ledger reports
    
    Args:
        ledger_path (str): folder which contains the ledger reports.
        outPath (str): folder where you want the output.
    """
    ledger_path = "Chemical/FOCUS/"
    files = os.listdir(ledger_path)
    try:
        for file in files:
            if file.endswith(".xlsx"): #check whether .xlsx file is being processed
                full_file_path = os.path.join(ledger_path, file)
                df = pd.read_excel(full_file_path, nrows=12)
                rows = df.astype(str).apply(lambda x: x.str.contains("date", case=False, na=False)).any(axis=1)
                rows_to_skip = int(rows.idxmax()) + 3
                df = pd.read_excel(full_file_path, skiprows=rows_to_skip)
                fileName = str(file).replace(".xlsx", "")
                for col in ["Debit", "Credit"]:
                    if col in df.columns:
                        df[col] = df[col].astype(float).fillna(0)
                for col in df.columns:
                    if col not in ["Debit", "Credit"]:
                        df[col] = df[col].astype(str)
                df.to_parquet(outPath+fileName+".parquet",index=False)
                del df
                gc.collect()
    except Exception as e:
        print(f"Failed to process {file}: {e}")
    print("All files processed and RAM cleared.")

#formatting the file into usable format
def formatting(srcPath: str,outPath:str):
    """here we perform the cleaning of the dataset and merging the two formats as both have difference usable columns.
    The function just outputs the file and returns nothing.
    Args:
        srcPath (str): path where the raw datasets exist in the parquet format. The files must have the word "search" in case of the finance search format file and the word "statement" in case of statement of account finance format.
        outPath (str): path where the output single file will be exported into xlsx format and parquet format.
    """
    files = os.listdir(srcPath)
    for file in files:
        full_file_path = os.path.join(srcPath, file)
        if file.endswith(".parquet"):
            if "search" in file.lower():
                df = pd.read_parquet(full_file_path, engine='pyarrow')
                df = df.groupby(["Date","Voucher", "Account Name", "Account Code", "Narration", 'Account Pan No']).agg({"Debit": "sum", "Credit": 'sum'}).reset_index()
            if "statement" in file.lower():
                #loading the statement of accounts finance format
                df1 = pd.read_parquet("Chemical/output/Chemical & Regeants Statement of Account Finance format.parquet")
                df1 = df1[df1["Voucher"].notna()] #removing blank rows
                df1 = df1.groupby(["Date", "Voucher", "BillNo", "Bill Date"]).agg({"Debit": 'sum', "Credit": 'sum'}).reset_index()
    df3 = pd.merge(left=df, right=df1[["Voucher", "BillNo", "Bill Date"]], how='left', on="Voucher")
    for key, value in regex_patterns.items():
        df3[key] = df3["Narration"].str.extract(value)[0]
    df3.to_parquet(outPath+".parquet", index=False)
    df3.to_excel(outPath+".xlsx", index=False)
    del df
    del df1
    del df3
    gc.collect() #clearing from RAM

def po_process(srcPath: str):
    """Processing the PO reports for further usage in Purhcase VS PO audit process
    Args:
        srcPath (str): The path where the raw PO reports exist
    
    Returns:
        po_reports = a pandas dataframe
    """
    files = os.listdir("Purchase Order Reports/")
    po_data = []
    for file in files:
        if not "until" in file:
            file_path = os.path.join("Purchase Order Reports/", file)
            names = ["Date", "doc", "vendor code", "vendor name", "item name", "unit name", "Description", "Quantity", "Rate"]
            po_report = pd.read_excel(file_path, skiprows=5)
            for i in range(len(names)):
                po_report.columns.values[i] = names[i]
            po_report = po_report.loc[:, :"Rate"]
            po_data.append(po_report)
    po_reports = pd.concat(po_data)
    return po_reports

def purchase_vs_po(po_reports, formatted_ledger):
    """Performs the comparison of formatted ledger vs PO Report
    
    Args:
        po_reports: pandas dataframe, typically the output of po_process function
        formatted_ledger: parquet file which is the output of formatting function
    """
    po_reports_unique = po_reports.drop_duplicates(subset="doc")
    purchase_vs_po = pd.merge(left=formatted_ledger, right=po_reports_unique, how="left", left_on="PO", right_on="doc", suffixes=(None, "as per PO report"))
    return purchase_vs_po

def purchase_vs_grn(srcFilePath:str, formatted_ledger):
    """Performs the Purchase vs GRN audit process.
    
    Args:
        srcFilePath: The actual file path of xlsx file. This must be combined GRN Report of all the plants.
        formatted_ledger: The parquet file which is the output of the formatting function.
    """
    df_grn_report = pd.read_excel(srcFilePath)
    df_grn_report.columns.values[5] = "PO"
    for key in ["GRN", "PO"]:
        df_grn_report[key] = df_grn_report[key].astype(str).str.strip()
    df_grn_unique = df_grn_report.drop_duplicates(subset=["GRN","PO"])
    purchase_vs_grn = pd.merge(left=formatted_ledger, right=df_grn_unique, how="left", on=["PO", "GRN"], suffixes=(None,"GRN_Report"))
    return purchase_vs_grn

def purchase_vs_gstr2a(formatted_ledger_File, gstr2ACombinedFile, outPath:str):
    df_invoices = pd.read_parquet(formatted_ledger_File)
    df_gstr2A = pd.read_excel(gstr2ACombinedFile)
    df_gstr2A["PAN"] = df_gstr2A["GSTIN"].astype(str).str.extract(r"([A-Z]{5}[0-9]{4}[A-Z]{1})")
    
    def fuzzyMatch(row, df_gstr2A_ref):
        query = row['BillNo'] # Ensure this is a string
        row_pan = row['Account Pan No']
        
        # 1. Filter by PAN
        df_to_look_at = df_gstr2A_ref[df_gstr2A_ref["PAN"] == row_pan]
        
        # 2. LOGICAL CHECK: If no invoices exist for this PAN, stop immediately.
        if df_to_look_at.empty:
            return pd.Series({
                'matched_gstr2a_invoice': None, 
                'match_percentage': 0,
                'matched_Invoice_value': 0, 
                'matched_Taxable_Value': 0
            })

        # 3. FIX: Pass the specific COLUMN (Series), not the whole DataFrame
        # Rapidfuzz will return (value, score, index)
        result = process.extractOne(
            query, 
            df_to_look_at["Invoice No"], # <--- SPECIFY THE COLUMN HERE
            score_cutoff=85, 
            scorer=fuzz.token_sort_ratio
        )

        if result is None:
            return pd.Series({
                'matched_gstr2a_invoice': None, 
                'match_percentage': 0,
                'matched_Invoice_value': 0, 
                'matched_Taxable_Value': 0
            })

        # 4. Use result[2] to locate the row in the original/filtered DF
        matched_row = df_to_look_at.loc[result[2]]

        return pd.Series({
            'matched_gstr2a_invoice': matched_row["Invoice No"], 
            'match_percentage': result[1],
            'matched_Invoice_value': matched_row['Invoice Value'],
            'matched_Taxable_Value': matched_row['Taxable Value'],
        })
    
    matched_df = df_invoices.apply(lambda row: fuzzyMatch(row, df_gstr2A),axis=1)
    df_invoices_matched = pd.concat([df_invoices, matched_df], axis =1)

    #exporting the separate excel files
    df_invoices_matched[~df_invoices_matched["BillNo"].astype(str).str.contains("chemical", case=False, na=False) & df_invoices_matched["matched_gstr2a_invoice"].notna()].to_excel(outPath+"matched chemical invoices with gstr2a.xlsx", index=False)
    df_invoices_matched[~df_invoices_matched["BillNo"].astype(str).str.contains("chemical", case=False, na=False) & df_invoices_matched["matched_gstr2a_invoice"].isna()].to_excel(outPath+"unmatched Invoices Chemical Purchases.xlsx", index=False)
    
    #Exporting the complete excel file
    df_invoices_matched.to_excel(outPath+"with gstr2a.xlsx", index=False)