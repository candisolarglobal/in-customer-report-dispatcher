import os
import pandas as pd

def process_customer_reports(folder_path, excel_filename):
    # Construct the full path to the Excel file
    excel_path = os.path.join(folder_path, excel_filename)

    # 1. Load Excel Data
    if not os.path.exists(excel_path):
        print(f"Error: '{excel_filename}' not found in '{folder_path}'")
        return

    # Loading the sheet
    df = pd.read_excel(excel_path)
    
    # 2. Map 'Project' to 'Primary Contact Email' 
    # (Matches your specific column names)
    email_map = dict(zip(df['Project'].astype(str).str.strip(), 
                         df['Primary Contact Email'].astype(str).str.strip()))

    # 3. List all PDFs in the folder
    files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]
    
    print(f"\n{'PDF Filename':<45} | {'Matched Email':<30}")
    print("-" * 80)

    matches = []

    # 4. Matching Logic
    # We sort by length (longest names first) to avoid "Tribune" matching "Tribune NRS"
    sorted_projects = sorted(email_map.keys(), key=len, reverse=True)
    
    for pdf_file in files:
        match_found = False
        for project_name in sorted_projects:
            # Check if PDF starts with the Project Name
            if pdf_file.lower().startswith(project_name.lower()):
                email = email_map[project_name]
                print(f"{pdf_file:<45} | {email:<30}")
                matches.append({"file": pdf_file, "email": email})
                match_found = True
                break
        
        if not match_found:
            print(f"{pdf_file:<45} | No Match Found")

    return matches

# ==========================================================
# ENTER YOUR DETAILS HERE
# ==========================================================
# 1. Enter the full path to your "Q4 Report-25" folder
# Example: "C:/Users/Name/Documents/Q4 Report-25" or just "Q4 Report-25"
TARGET_FOLDER = r'G:\Shared drives\05_Asset Management_IN\02_Commercial AM\04. Client communication AM\Customer Reports\Q4 Report-25' 

# 2. Enter the exact name of your Excel file (include .xlsx)
EXCEL_FILE_NAME = 'Customer IDS.xlsx'
# ==========================================================

if __name__ == "__main__":
    process_customer_reports(TARGET_FOLDER, EXCEL_FILE_NAME)