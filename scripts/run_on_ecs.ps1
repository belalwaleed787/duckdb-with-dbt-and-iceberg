<#
.SYNOPSIS
    Starts one pipeline run on ECS Fargate, streams its logs and returns the container's exit code.

.EXAMPLE
    .\scripts\run_on_ecs.ps1                                        # dbt build
    .\scripts\run_on_ecs.ps1 -DbtArgs build, --select, gold         # any dbt command
    .\scripts\run_on_ecs.ps1 -DbtArgs build, --full-refresh
    .\scripts\run_on_ecs.ps1 -Environment @{ DBT_THREADS = "2" }    # extra environment variables
    .\scripts\run_on_ecs.ps1 -Spot                                  # Fargate Spot (~70% cheaper, can be interrupted)
    .\scripts\run_on_ecs.ps1 -NoWait                                # start it and return
#>
param(
    [string[]]$DbtArgs = @("build"),
    [hashtable]$Environment = @{},
    [string]$Region = "us-east-1",
    [string]$StackName = "nyc-taxi-duckdb-dbt",
    [switch]$Spot,
    [switch]$NoWait
)

function Assert-Success([string]$Step) {
    if ($LASTEXITCODE -ne 0) { throw "$Step failed (exit code $LASTEXITCODE)" }
}

$outputs = @{}
$stackOutputs = aws cloudformation describe-stacks --region $Region --stack-name $StackName `
    --query "Stacks[0].Outputs" --output json
Assert-Success "aws cloudformation describe-stacks"
($stackOutputs | Out-String | ConvertFrom-Json) | ForEach-Object { $outputs[$_.OutputKey] = $_.OutputValue }

$containerEnvironment = @($Environment.GetEnumerator() | ForEach-Object { @{ name = $_.Key; value = [string]$_.Value } })
$overrides = @{
    containerOverrides = @(
        @{ name = $outputs.ContainerName; command = @($DbtArgs); environment = $containerEnvironment }
    )
}
$overridesFile = [System.IO.Path]::GetTempFileName()
[System.IO.File]::WriteAllText($overridesFile, ($overrides | ConvertTo-Json -Depth 6))

$network = "awsvpcConfiguration={subnets=[$($outputs.SubnetIds)],securityGroups=[$($outputs.SecurityGroupId)],assignPublicIp=ENABLED}"
if ($Spot) {
    $capacity = @("--capacity-provider-strategy", "capacityProvider=FARGATE_SPOT,weight=1")
} else {
    $capacity = @("--launch-type", "FARGATE")
}
$taskArn = aws ecs run-task --region $Region --cluster $outputs.ClusterName `
    --task-definition $outputs.TaskDefinition @capacity `
    --network-configuration $network --overrides "file://$overridesFile" `
    --query "tasks[0].taskArn" --output text
$runTaskExit = $LASTEXITCODE
Remove-Item $overridesFile
if ($runTaskExit -ne 0 -or -not $taskArn -or $taskArn -eq "None") { throw "aws ecs run-task failed" }

$taskId = $taskArn.Split("/")[-1]
$logStream = "run/$($outputs.ContainerName)/$taskId"
Write-Host "Started task $taskId (logs: $($outputs.LogGroupName) / $logStream)"
if ($NoWait) { return }

# Print new log lines every 10 seconds until the task has stopped.
$nextToken = $null
function Show-NewLogLines {
    $logArgs = @("logs", "get-log-events", "--region", $Region, "--log-group-name", $outputs.LogGroupName,
        "--log-stream-name", $logStream, "--start-from-head", "--output", "json")
    if ($script:nextToken) { $logArgs += @("--next-token", $script:nextToken) }
    $page = aws @logArgs 2>$null
    if ($LASTEXITCODE -eq 0 -and $page) {
        $page = $page | Out-String | ConvertFrom-Json
        $page.events | ForEach-Object { Write-Host $_.message }
        $script:nextToken = $page.nextForwardToken
    }
}
do {
    Start-Sleep -Seconds 10
    Show-NewLogLines
    $lastStatus = aws ecs describe-tasks --region $Region --cluster $outputs.ClusterName --tasks $taskArn `
        --query "tasks[0].lastStatus" --output text
} while ($lastStatus -ne "STOPPED")
Start-Sleep -Seconds 5
Show-NewLogLines

$task = aws ecs describe-tasks --region $Region --cluster $outputs.ClusterName --tasks $taskArn `
    --query "tasks[0]" --output json | Out-String | ConvertFrom-Json
$exitCode = $task.containers[0].exitCode
Write-Host "Task stopped ($($task.stoppedReason)); container exit code: $exitCode"
if ($exitCode -ne 0) { exit 1 }
