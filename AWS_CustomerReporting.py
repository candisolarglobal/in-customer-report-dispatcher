import boto3
import openpyxl
from io import BytesIO

s3 = boto3.client('s3')
ses = boto3.client('ses')

def lambda_handler(event, context):
    # --- 1. CONFIGURATION ---
    BUCKET_NAME = "your-s3-bucket-name" # Enter your actual bucket name here
    EXCEL_FILE_KEY = 'Customer IDS.xlsx' 
    SENDER_EMAIL = "accountsreceivables-IN@candi.solar"

    # --- 2. LOAD THE EXCEL FILE ---
    try:
        excel_obj = s3.get_object(Bucket=BUCKET_NAME, Key=EXCEL_FILE_KEY)
        wb = openpyxl.load_workbook(BytesIO(excel_obj['Body'].read()))
        sheet = wb.active
        
        # Create the mapping from the Excel sheet
        email_map = {}
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if row[0] and row[1]:
                email_map[str(row[0]).strip()] = str(row[1]).strip()
    except Exception as e:
        print(f"Error loading Excel: {e}")
        return

    # --- 3. LIST ALL PDFS IN THE BUCKET ---
    # This retrieves a list of every file currently in S3
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    if 'Contents' not in response:
        print("No files found in bucket.")
        return

    # Sort project names by length (longest first) for accurate matching
    sorted_projects = sorted(email_map.keys(), key=len, reverse=True)

    # --- 4. LOOP THROUGH EVERY FILE IN S3 ---
    for obj in response['Contents']:
        pdf_key = obj['Key']
        
        # Skip the Excel file itself and non-PDFs
        if not pdf_key.lower().endswith('.pdf'):
            continue

        match_found = False
        
        # --- 5. MATCHING LOGIC FOR THIS SPECIFIC FILE ---
        for project_name in sorted_projects:
            if pdf_key.lower().startswith(project_name.lower()):
                recipient_email = email_map[project_name]
                
                # --- 6. SEND EMAIL ---
                send_report_email(recipient_email, SENDER_EMAIL, pdf_key)
                print(f"SUCCESS: Matched {pdf_key} to {recipient_email}")
                match_found = True
                break # Move to the next PDF once matched
        
        if not match_found:
            print(f"NO MATCH: Could not find project for {pdf_key}")

def send_report_email(to_address, from_address, filename):
    subject = f"Quarterly Report Available: {filename}"
    body = f"Hello,\n\nYour quarterly report ({filename}) is now available."
    
    ses.send_email(
        Source=from_address,
        Destination={'ToAddresses': [to_address]},
        Message={
            'Subject': {'Data': subject},
            'Body': {'Text': {'Data': body}}
        }
    )