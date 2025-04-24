# Create a virtual environment and install dependencies
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Create deployment package
$env:PYTHONPATH = "$PWD"
Compress-Archive -Path "process_events.py" -DestinationPath "function.zip" -Force
Compress-Archive -Path ".venv\Lib\site-packages\*" -DestinationPath "function.zip" -Update

Write-Host "Deployment package created: function.zip"
Write-Host "You can now upload this package to AWS Lambda through the console or using AWS CLI"

# Optional: Deploy using AWS CLI (uncomment and configure if needed)
# $FUNCTION_NAME = "process-playback-events-dev"
# $ROLE_ARN = "YOUR_LAMBDA_ROLE_ARN"
# aws lambda create-function `
#     --function-name $FUNCTION_NAME `
#     --runtime python3.9 `
#     --handler process_events.lambda_handler `
#     --role $ROLE_ARN `
#     --zip-file fileb://function.zip

# To update an existing function:
# aws lambda update-function-code `
#     --function-name $FUNCTION_NAME `
#     --zip-file fileb://function.zip 