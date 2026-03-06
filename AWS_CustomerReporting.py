import boto3
import openpyxl
from io import BytesIO
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import json

s3 = boto3.client('s3')
ses = boto3.client('ses')

def get_gdrive_service(service_account_cred_string, scopes=['https://www.googleapis.com/auth/drive.metadata.readonly']):
    """Authenticates using a service account JSON file."""

    # Convert the string into a dictionary
    info = json.loads(service_account_cred_string)

    creds = service_account.Credentials.from_service_account_info(
        info, scopes=scopes)
    return build('drive', 'v3', credentials=creds)

def get_gdrive_folder_id_by_path(service, path, parent_id):
    """
    Finds the ID of the last folder in a path string.
    path: 'Folder1/SubFolder2'
    parent_id: parent folder id
    """
    parts = [p for p in path.split('/') if p]

    for part in parts:
        # Note: 'parents' check uses the 'in' operator with the parent_id string
        query = (f"name = '{part}' and "
                 f"mimeType = 'application/vnd.google-apps.folder' and "
                 f"'{parent_id}' in parents and "
                 f"trashed = false")
        
        results = service.files().list(
            q=query,
            spaces='drive',
            fields="files(id, name)",
            supportsAllDrives=True,          
            includeItemsFromAllDrives=True,  
            corpora='allDrives'
        ).execute()
        
        items = results.get('files', [])
        
        if not items:
            raise Exception(f"Folder '{part}' not found under parent ID '{parent_id}'")
        
        # Move the pointer to the found folder's ID for the next iteration
        parent_id = items[0]['id']
        
    return parent_id

def list_gdrive_files_in_folder(service, folder_id):
    """Returns a list of (name, id) tuples for files in a specific folder."""
    query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(
        q=query, 
        fields="files(id, name)",
        supportsAllDrives=True,          
        includeItemsFromAllDrives=True,  
        corpora='allDrives'
    ).execute()
    return results.get('files', [])

def download_file_content(service, file_id):
    """Downloads file content into memory."""
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()

def get_ssm_param(ssm_param_name, region_name="eu-central-1", WithDecryption=True):
    
    ssm_client = boto3.client("ssm", region_name=region_name) #s.environ["AWS_REGION"]
    response = ssm_client.get_parameter(
        Name=ssm_param_name,
        WithDecryption=WithDecryption  # Set to True if the parameter is a SecureString
    )
    parameter_value = response["Parameter"]["Value"]
    return parameter_value
    

def lambda_handler(event, context):
    # --- 1. CONFIGURATION ---
    BUCKET_NAME = "in-customer-reports" 
    EXCEL_FILE_KEY = 'Customer_IDS.xlsx' 
    SENDER_EMAIL = "accountsreceivables-in@candi.solar"
    GDRIVE_SERVICE_ACCOUNT_KEY = get_ssm_param("/general/AMGDriveAccountKey") #JSON string
    Gdrive_service = get_gdrive_service(GDRIVE_SERVICE_ACCOUNT_KEY)

    FOLDER_ID_CUST_REPORTS = "1h4N3hiPy9gKEv2fYYveaKbMrhzSv8oXo"
    #FOLDER_ID_CUST_REPORTS = "1pYHVtj2OWfXOy61g_ryLXi-hI6E-bZr-"

    QUARTER = "Q4"
    YEAR= 2025
    QUARTER_REPORT_FOLDER_NAME = f'{QUARTER}_Report_{YEAR}'

    FOLDER_ID_QUARTER_REPORT = get_gdrive_folder_id_by_path(Gdrive_service, QUARTER_REPORT_FOLDER_NAME, FOLDER_ID_CUST_REPORTS)


    FILES_LIST_TEST = list_gdrive_files_in_folder(Gdrive_service, FOLDER_ID_QUARTER_REPORT)

    print(FILES_LIST_TEST)
    
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



