<#
.SYNOPSIS
    Builds the pipeline image (linux/amd64) and pushes it to Amazon ECR.
    Creates the ECR repository the first time.

.EXAMPLE
    .\scripts\push_to_ecr.ps1
    .\scripts\push_to_ecr.ps1 -Region us-east-1 -Repository nyc-taxi-duckdb-dbt -Tag v1
#>
param(
    [string]$Region = "us-east-1",
    [string]$Repository = "nyc-taxi-duckdb-dbt",
    [string]$Tag = "latest"
)

function Assert-Success([string]$Step) {
    if ($LASTEXITCODE -ne 0) { throw "$Step failed (exit code $LASTEXITCODE)" }
}

$projectRoot = Split-Path -Parent $PSScriptRoot

$account = aws sts get-caller-identity --query Account --output text
Assert-Success "aws sts get-caller-identity"
$registry = "$account.dkr.ecr.$Region.amazonaws.com"
$image = "$registry/${Repository}:$Tag"

aws ecr describe-repositories --region $Region --repository-names $Repository *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Creating ECR repository $Repository in $Region"
    aws ecr create-repository --region $Region --repository-name $Repository `
        --image-scanning-configuration scanOnPush=true | Out-Null
    Assert-Success "aws ecr create-repository"
}

# Piped through cmd: Windows PowerShell 5.1 would append "\r\n" to the password and ECR rejects it.
cmd /c "aws ecr get-login-password --region $Region | docker login --username AWS --password-stdin $registry"
Assert-Success "docker login"

docker build --platform linux/amd64 -t $image $projectRoot
Assert-Success "docker build"

docker push $image
Assert-Success "docker push"

Write-Host "Pushed $image"
