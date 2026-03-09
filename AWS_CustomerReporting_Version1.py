import boto3
import openpyxl
import io
import json
from datetime import datetime, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


ses = boto3.client('ses')


def get_current_quarter_and_year():
    """Derives the current quarter and year from today's date."""
    now = datetime.now(timezone.utc)
    year = now.year
    month = now.month
    quarter = f"Q{(month - 1) // 3 + 1}"
    return quarter, year


def get_gdrive_service(service_account_cred_string, scopes=['https://www.googleapis.com/auth/drive.readonly']):
    """Authenticates using a service account JSON string."""
    info = json.loads(service_account_cred_string)
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return build('drive', 'v3', credentials=creds)


def get_gdrive_folder_id_by_path(service, path, parent_id):
    """
    Finds the ID of the last folder in a path string.
    path: 'Folder1/SubFolder2'
    parent_id: parent folder id
    """
    parts = [p for p in path.split('/') if p]

    for part in parts:
        query = (
            f"name = '{part}' and "
            f"mimeType = 'application/vnd.google-apps.folder' and "
            f"'{parent_id}' in parents and "
            f"trashed = false"
        )
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

        parent_id = items[0]['id']

    return parent_id


def list_gdrive_files_in_folder(service, folder_id):
    """Returns a list of file metadata dicts (id, name) for all files in a folder."""
    query = (
        f"'{folder_id}' in parents and "
        f"mimeType != 'application/vnd.google-apps.folder' and "
        f"trashed = false"
    )
    results = service.files().list(
        q=query,
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora='allDrives'
    ).execute()
    return results.get('files', [])


def download_file_content(service, file_id):
    """Downloads a file from Google Drive into memory and returns bytes."""
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()


def get_ssm_param(ssm_param_name, region_name="eu-central-1", WithDecryption=True):
    ssm_client = boto3.client("ssm", region_name=region_name)
    response = ssm_client.get_parameter(
        Name=ssm_param_name,
        WithDecryption=WithDecryption
    )
    return response["Parameter"]["Value"]


def send_report_email(to_address, from_address, filename, file_bytes):
    """Sends an email via SES with the PDF attached."""
    import email.mime.multipart as mp
    import email.mime.text as mt
    import email.mime.application as ma

    msg = mp.MIMEMultipart()
    msg['Subject'] = f"Your Quarterly Report: {filename}"
    msg['From'] = from_address
    msg['To'] = to_address

    body = (
        f"Hello,\n\n"
        f"Please find your quarterly report ({filename}) attached.\n\n"
        f"Best regards,\nAccounts Receivables Team"
    )
    msg.attach(mt.MIMEText(body, 'plain'))

    # Attach the PDF
    attachment = ma.MIMEApplication(file_bytes, _subtype='pdf')
    attachment.add_header('Content-Disposition', 'attachment', filename=filename)
    msg.attach(attachment)

    ses.send_raw_email(
        Source=from_address,
        Destinations=[to_address],
        RawMessage={'Data': msg.as_bytes()}
    )


def lambda_handler(event, context):
    # --- 1. CONFIGURATION ---
    SENDER_EMAIL = "accountsreceivables-in@candi.solar"
    EXCEL_FILENAME = "Customer IDS"

    GDRIVE_SERVICE_ACCOUNT_KEY = get_ssm_param("/general/AMGDriveAccountKey")
    gdrive_service = get_gdrive_service(GDRIVE_SERVICE_ACCOUNT_KEY)

    # Root folder that contains all quarter report subfolders
    FOLDER_ID_CUST_REPORTS = "1h4N3hiPy9gKEv2fYYveaKbMrhzSv8oXo" #------> Need to update this

    # --- 2. AUTO-DETECT QUARTER AND YEAR ---
    quarter, year = get_current_quarter_and_year()
    quarter_report_folder_name = f"{quarter}_Report_{year}"
    print(f"Looking for folder: {quarter_report_folder_name}")

    # --- 3. RESOLVE QUARTER FOLDER ID ---
    try:
        folder_id_quarter = get_gdrive_folder_id_by_path(
            gdrive_service, quarter_report_folder_name, FOLDER_ID_CUST_REPORTS
        )
    except Exception as e:
        print(f"Could not find quarter folder '{quarter_report_folder_name}': {e}")
        return

    # --- 4. LIST ALL FILES IN THE QUARTER FOLDER ---
    all_files = list_gdrive_files_in_folder(gdrive_service, folder_id_quarter)
    print(f"Found {len(all_files)} file(s) in folder: {[f['name'] for f in all_files]}")

    # --- 5. SEPARATE EXCEL FILE FROM PDFs ---
    excel_file_meta = None
    pdf_files = []

    for f in all_files:
        name_lower = f['name'].lower()
        # Match "Customer IDS" or "Customer IDS.xlsx"
        if name_lower == EXCEL_FILENAME.lower() or name_lower == EXCEL_FILENAME.lower() + '.xlsx':
            excel_file_meta = f
        elif name_lower.endswith('.pdf'):
            pdf_files.append(f)

    if not excel_file_meta:
        print(f"ERROR: Could not find '{EXCEL_FILENAME}' in the quarter folder.")
        return

    if not pdf_files:
        print("No PDF files found in the quarter folder.")
        return

    # --- 6. DOWNLOAD AND PARSE THE EXCEL FILE ---
    try:
        excel_bytes = download_file_content(gdrive_service, excel_file_meta['id'])
        wb = openpyxl.load_workbook(io.BytesIO(excel_bytes))
        sheet = wb["Data"]

        # Build mapping: customer_name (col A) -> recipient_email (col B)
        email_map = {}
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if row[0] and row[1]:
                email_map[str(row[0]).strip()] = str(row[1]).strip()

        print(f"Loaded {len(email_map)} customer entries from Excel.")
    except Exception as e:
        print(f"Error loading Excel from Drive: {e}")
        return

    # --- 7. MATCH EACH PDF TO A CUSTOMER AND SEND EMAIL ---
    # PDF filename format: "Customer Name_something.pdf"
    # Extract everything before the FIRST underscore as the customer name,
    # then do a case-insensitive exact match against Excel customer names.
    for pdf_file in pdf_files:
        pdf_name = pdf_file['name']
        pdf_stem = pdf_name.rsplit('.', 1)[0]                    # strip .pdf extension
        customer_name_from_pdf = pdf_stem.split('_')[0].strip()  # part before first '_'

        print(f"PDF: '{pdf_name}' → extracted customer name: '{customer_name_from_pdf}'")

        # Case-insensitive exact match against Excel customer names
        matched_key = next(
            (k for k in email_map if k.lower() == customer_name_from_pdf.lower()),
            None
        )

        if matched_key:
            recipient_email = email_map[matched_key]
            try:
                pdf_bytes = download_file_content(gdrive_service, pdf_file['id'])
                send_report_email(recipient_email, SENDER_EMAIL, pdf_name, pdf_bytes)
                print(f"SUCCESS: Sent '{pdf_name}' to {recipient_email}")
            except Exception as e:
                print(f"ERROR: Failed to send '{pdf_name}' to {recipient_email}: {e}")
        else:
            print(f"NO MATCH: No Excel entry for customer '{customer_name_from_pdf}' (from '{pdf_name}')")