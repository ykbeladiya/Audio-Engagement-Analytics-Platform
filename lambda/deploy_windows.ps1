# Configuration
$ENVIRONMENT = "dev"
$STACK_NAME = "audio-analytics-$ENVIRONMENT"
$REGION = "us-east-1"
$S3_BUCKET = "your-deployment-bucket"  # Replace with your S3 bucket
$S3_PREFIX = "lambda-functions"

# Create deployment package
Write-Host "Creating deployment package..."
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Create zip file
Compress-Archive -Path "process_events.py" -DestinationPath "function.zip" -Force
Get-ChildItem -Path ".venv\Lib\site-packages\*" | Compress-Archive -DestinationPath "function.zip" -Update

# Upload to S3
Write-Host "Uploading Lambda package to S3..."
aws s3 cp function.zip "s3://$S3_BUCKET/$S3_PREFIX/function.zip"

# Deploy CloudFormation stack
Write-Host "Deploying CloudFormation stack..."
aws cloudformation deploy `
    --template-file cloudformation.yaml `
    --stack-name $STACK_NAME `
    --parameter-overrides `
        Environment=$ENVIRONMENT `
        LambdaS3Bucket=$S3_BUCKET `
        LambdaS3Key="$S3_PREFIX/function.zip" `
    --capabilities CAPABILITY_NAMED_IAM `
    --region $REGION

# Get stack outputs
Write-Host "Stack outputs:"
aws cloudformation describe-stacks `
    --stack-name $STACK_NAME `
    --query 'Stacks[0].Outputs' `
    --output table `
    --region $REGION

Write-Host "Deployment complete!" 