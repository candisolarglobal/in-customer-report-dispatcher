# India Customer Report Dispatcher
Python Codes and walk - through of sending the quarterly customer reports

 ---
**Background**

Reports are first generated on Google LookerStudio. Via a Python function, they are auto-downloaded into the folder Client Communication/Customer Reports.
Every quarter, they are to be sent to the customer via mail. This repo aims to provide a means to automate that by leveraging AWS functionality.

---
**AWS Functionality**

1.  ***Lambda*** - Used for implementing Python codes - for matching customer_name in PDFs to customer_name in Excel Database. Also included GDrive Authentication code for providing access to stored PDFs.
2.  ***S3*** - Think of it like a storage bucket. Stores the Excel Database and the PDFs to be sent.
3.  ***SES*** - Simple Email Service - The function used to send the emails
4.  ***EventBridge*** - Used to automate the sending process. It is like a cron job and can be pre-set.
5.  ***IAM*** - Identity and Access Management - Used for authentication purposes.

---
Python code for an aws lambda function to dispatch india customer reports on a quarterly basis via email
