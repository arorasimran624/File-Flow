FileFlow is a data processing and validation pipeline built to handle CSV files efficiently. It ensures that data is clean, accurate, and ready to use before moving forward in the workflow.

How It Works

The process begins with file input, where a CSV file is uploaded or sent to the system. 

Next comes processing and validation. In this stage, it performs checks to see data quality. The template check detects if any columns are missing, extra, or arranged in the wrong order. The null check scans for rows with empty or missing values. Finally, row validation verifies specific fields inside each row: the contact number must contain exactly 10 digits, the email must include the “@” symbol, and the date of birth must follow a valid date format.

After validation, FileFlow moves to the status update stage. Here, the result of the validation — whether successful or failed, along with detailed error information — is sent as a notification to Microsoft Teams. 

Flow:
CSV File → Processing & Validation → Status → Teams Notification
