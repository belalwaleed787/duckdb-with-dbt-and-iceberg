<#
.SYNOPSIS
    Creates or updates the ECS Fargate setup (infra/ecs-stack.yaml) in the default VPC:
    ECR repository, ECS cluster, task definition, IAM roles, security group and log group.
    Nothing is scheduled; start runs with scripts/run_on_ecs.ps1.

.EXAMPLE
    .\scripts\deploy_ecs.ps1
    .\scripts\deploy_ecs.ps1 -TaskCpu 2048 -TaskMemory 8192
#>
param(
    [string]$Region = "us-east-1",
    [string]$StackName = "nyc-taxi-duckdb-dbt",
    [string]$BucketName = "nyc-test-iceberg",
    [string]$TaskCpu = "4096",
    [string]$TaskMemory = "16384"
)

function Assert-Success([string]$Step) {
    if ($LASTEXITCODE -ne 0) { throw "$Step failed (exit code $LASTEXITCODE)" }
}

# ECS needs its service-linked role once per account; it is missing if ECS was never used.
aws iam get-role --role-name AWSServiceRoleForECS *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Creating the ECS service-linked role (one time per account)"
    aws iam create-service-linked-role --aws-service-name ecs.amazonaws.com | Out-Null
    Assert-Success "aws iam create-service-linked-role"
    Start-Sleep -Seconds 15
}

# A first deployment that failed leaves the stack in ROLLBACK_COMPLETE; it must be deleted before retrying.
$stackStatus = aws cloudformation describe-stacks --region $Region --stack-name $StackName `
    --query "Stacks[0].StackStatus" --output text 2> $null
if ($stackStatus -eq "ROLLBACK_COMPLETE") {
    Write-Host "Removing the failed first deployment of $StackName"
    aws cloudformation delete-stack --region $Region --stack-name $StackName
    aws cloudformation wait stack-delete-complete --region $Region --stack-name $StackName
    Assert-Success "aws cloudformation delete-stack"
}

$vpcId = aws ec2 describe-vpcs --region $Region --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text
Assert-Success "aws ec2 describe-vpcs"

# Default (public) subnets of the VPC, skipping use1-az3 where Fargate is not available.
$subnets = aws ec2 describe-subnets --region $Region `
    --filters Name=vpc-id,Values=$vpcId Name=default-for-az,Values=true `
    --query "Subnets[?AvailabilityZoneId!='use1-az3'].SubnetId" --output text
Assert-Success "aws ec2 describe-subnets"
$subnetList = ($subnets -split "\s+" | Where-Object { $_ }) -join ","

$template = Join-Path (Split-Path -Parent $PSScriptRoot) "infra\ecs-stack.yaml"
aws cloudformation deploy --region $Region --stack-name $StackName --template-file $template `
    --capabilities CAPABILITY_NAMED_IAM --no-fail-on-empty-changeset `
    --parameter-overrides "ProjectName=$StackName" "BucketName=$BucketName" "VpcId=$vpcId" `
    "SubnetIds=$subnetList" "TaskCpu=$TaskCpu" "TaskMemory=$TaskMemory"
Assert-Success "aws cloudformation deploy"

aws cloudformation describe-stacks --region $Region --stack-name $StackName --query "Stacks[0].Outputs" --output table
