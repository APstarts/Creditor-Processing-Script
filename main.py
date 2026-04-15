import audit_methods as at
import pandas as pd

#converting the raw files into parquet files
at.convert_to_partquet(ledger_path="../Creditor for goods/focus/", outPath="../Creditor for goods/output/")

# formatting and cleaning of the file for further usage in pipeline
formatted_ledger = at.formatting(srcPath="../Creditor for goods/output/", outPath="../Creditor for goods/formatted/")

# getting the po_reports dataframe
po_reports = at.po_process(srcPath="../Purchase Order Reports/")

#performing the purchases vs po process
purchase_vs_po = at.purchase_vs_po(po_reports=po_reports, formatted_ledger=formatted_ledger)

#Performing the purchase vs GRN process
purchase_vs_grn = at.purchase_vs_grn(srcFilePath="../GRN Reports/Revised Combined GRN Reports.xlsx", formatted_ledger=formatted_ledger)

# Performing the purchase vs GSTR2A
at.purchase_vs_gstr2a(gstr2ACombinedFile="../GSTR 2A/combined_2A.xlsx", formatted_ledger=formatted_ledger, outPath="../Creditor for goods/")

with pd.ExcelWriter("../Creditor for goods/PO + GRN Reconciled.xlsx", engine="openpyxl") as writer:
    formatted_ledger.to_excel(writer, sheet_name="formatted Ledger with PO GRN Extracted", index=False)
    purchase_vs_po.to_excel(writer, sheet_name="purchase_vs_po", index=False)
    purchase_vs_grn.to_excel(writer, sheet_name="purchase_vs_grn", index=False)