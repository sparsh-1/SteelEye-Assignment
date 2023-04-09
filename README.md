# SteelEye-Assignment

Link to assessment :- https://github.com/steeleye/recruitment-ext/wiki/Python-Engineer-Assessment

This repository have the solution to the assignment (link provided above)

## Dependencies
* beautifulsoup
* logging
* requests
* os
* zipfile
* pandas
* boto3

## Procedure
1. Install and configure aws in your system and provide the asw secret access key, region in the terminal. 
2. Provide the bucket name and the access_key in the main.py file
3. Then run the main.py file which will automatically extracts the URL, download ZIP file, unzip it, convert it to CSV, and uploads it to the S3 bucket.
