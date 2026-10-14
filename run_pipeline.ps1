<#
.SYNOPSIS
  Automated PowerShell runner for Clickstream MapReduce pipeline on Windows.

.EXAMPLE
  .\run_pipeline.ps1
  .\run_pipeline.ps1 -Action build
  .\run_pipeline.ps1 -Action run -Date 2019-11-01
  .\run_pipeline.ps1 -Action stop
#>

param(
    [ValidateSet("all", "build", "start", "split", "ingest", "run", "verify", "dashboard", "stop", "status")]
    [string]$Action = "all",

    [string]$Date = "2019-11-01",
    [int]$Rows = 2000000
)

$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot

function Log([string]$msg) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$timestamp] $msg" -ForegroundColor Cyan
}

function Build-Jar {
    Log "Building clickstream-mr.jar with Maven..."
    if (Get-Command mvn -ErrorAction SilentlyContinue) {
        mvn -DskipTests package
    } else {
        Log "mvn not found on host PATH. Using official Maven Docker container..."
        docker run --rm -v "${Root}:/app" -w /app maven:3.9-eclipse-temurin-8 mvn -DskipTests package
    }
    if (Test-Path "$Root\target\clickstream-mr.jar") {
        Log "Build succeeded: target/clickstream-mr.jar"
    } else {
        throw "Failed to create target/clickstream-mr.jar"
    }
}

function Start-Cluster {
    Log "Starting Hadoop cluster (HDFS + YARN) via Docker Compose..."
    docker compose up -d
    Log "Waiting for cluster services to be ready..."
    Start-Sleep -Seconds 10
    docker compose exec resourcemanager hdfs dfsadmin -safemode leave
    docker compose ps
    Log "Cluster is UP. Web UIs: HDFS http://localhost:9870 | YARN http://localhost:8088"
}

function Split-Data {
    $src = "$Root\data\2019-Nov.csv"
    if (-not (Test-Path $src)) {
        Log "data/2019-Nov.csv not found. Checking for sample data..."
        if (-not (Test-Path "$Root\data\sample_clickstream.csv")) {
            python "$Root\scripts\generate_sample_data.py" --out "$Root\data\sample_clickstream.csv"
        }
        $src = "$Root\data\sample_clickstream.csv"
    }
    Log "Splitting daily partition from $src (up to $Rows rows)..."
    python "$Root\scripts\split_by_day.py" $src "$Root\data\daily" $Rows
}

function Ingest-HDFS {
    $csv = "$Root\data\daily\$Date.csv"
    if (-not (Test-Path $csv)) {
        throw "Partition file $csv does not exist. Run .\run_pipeline.ps1 -Action split first."
    }
    Log "Uploading $csv to HDFS (/clickstream/raw/dt=$Date/)..."
    docker compose exec resourcemanager bash -c "cd /project && scripts/hdfs_ingest.sh data/daily/$Date.csv $Date"
}

function Run-Nightly {
    Log "Submitting MapReduce batch jobs to YARN (queue: batch, partition: $Date)..."
    docker compose exec resourcemanager bash -c "cd /project && REDUCERS=2 scripts/run_nightly.sh $Date"
}

function Verify-Results {
    $csv = "$Root\data\daily\$Date.csv"
    $rep = "$Root\reports\$Date"
    if ((Test-Path $csv) -and (Test-Path $rep)) {
        Log "Verifying MapReduce reports against independent Python computation..."
        python "$Root\scripts\verify_results.py" --input $csv --reports $rep
    } else {
        Log "Skipping verification (input or report missing for $Date)."
    }
}

function Build-Dashboard {
    Log "Building batch dashboard..."
    python "$Root\scripts\build_dashboard.py" --reports "$Root\reports" --out "$Root\dashboard\index.html"
    Log "Dashboard ready: dashboard/index.html"
}

function Stop-Cluster {
    Log "Stopping Hadoop Docker cluster..."
    docker compose down
    Log "Cluster stopped."
}

function Check-Status {
    docker compose ps
    docker compose exec resourcemanager yarn node -list
    docker compose exec resourcemanager yarn application -list
}

switch ($Action) {
    "build"     { Build-Jar }
    "start"     { Start-Cluster }
    "split"     { Split-Data }
    "ingest"    { Ingest-HDFS }
    "run"       { Run-Nightly }
    "verify"    { Verify-Results }
    "dashboard" { Build-Dashboard }
    "stop"      { Stop-Cluster }
    "status"    { Check-Status }
    "all" {
        if (-not (Test-Path "$Root\target\clickstream-mr.jar")) { Build-Jar }
        Start-Cluster
        if (-not (Test-Path "$Root\data\daily\$Date.csv")) { Split-Data }
        Ingest-HDFS
        Run-Nightly
        Verify-Results
        Build-Dashboard
        Log "PIPELINE COMPLETED SUCCESSFULLY!"
        Log "View Web UIs:"
        Log "  * HDFS NameNode: http://localhost:9870"
        Log "  * YARN ResourceManager: http://localhost:8088"
        Log "  * MapReduce JobHistory: http://localhost:19888"
        Log "  * Local Batch Dashboard: dashboard/index.html"
    }
}
