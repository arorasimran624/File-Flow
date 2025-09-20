FileFlow is a data processing and validation pipeline built to handle CSV files efficiently. 

How It Works

1.File Input
Upload or send a CSV file to the system.

2.Processing & Validation
Template Check: It detects if any columns are missing, extra, or in the wrong order.
Null Check: It also flags rows with empty or missing values.
Row Validation: It Validates fields such as
  Contact number must be of 10 digits
  Email format must contain @
  Date of Birth valid date format

3.Status Update
After validation, the status (success/failure with details) is sent to Teams Notification for tracking.

Flow
CSV File → Processing & Validation → Status → Teams Notification
