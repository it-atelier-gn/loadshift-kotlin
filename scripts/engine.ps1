param(
    [Parameter(Mandatory = $true)][ValidateSet("c7", "c8")][string]$Engine,
    [ValidateSet("start", "stop", "logs")][string]$Action = "start",
    [int]$Port = 8080
)

$name = if ($Engine -eq "c7") { "loadshift-dev-camunda7" } else { "loadshift-dev-camunda8" }

if ($Action -eq "stop") {
    docker rm -f $name | Out-Null
    Write-Host "$name stopped"
    exit 0
}

if ($Action -eq "logs") {
    docker logs -f $name
    exit $LASTEXITCODE
}

docker rm -f $name 2>$null | Out-Null

if ($Engine -eq "c7") {
    docker run -d --name $name -p "${Port}:8080" camunda/camunda-bpm-platform:run-7.24.0 | Out-Null
    $probe = "http://localhost:$Port/engine-rest/version"
} else {
    $config = Join-Path $PSScriptRoot "c8-application.yaml"
    docker run -d --name $name -p "${Port}:8080" -p "26500:26500" `
        -v "${config}:/usr/local/camunda/config/application.yaml:ro" `
        camunda/camunda:8.9.8 | Out-Null
    $probe = "http://localhost:$Port/v2/topology"
}

Write-Host "waiting for $name ..."
foreach ($i in 1..120) {
    try {
        Invoke-WebRequest $probe -UseBasicParsing -TimeoutSec 2 | Out-Null
        break
    } catch {
        if ($i -eq 120) {
            Write-Error "engine did not become ready; check: docker logs $name"
            exit 1
        }
        Start-Sleep -Seconds 2
    }
}

if ($Engine -eq "c7") {
    Write-Host ""
    Write-Host "Camunda 7 ready."
    Write-Host "  REST     http://localhost:$Port/engine-rest"
    Write-Host "  Cockpit  http://localhost:$Port/camunda  (demo/demo)"
    Write-Host ""
    Write-Host "  val backend = Camunda7Backend(`"http://localhost:$Port/engine-rest`")"
} else {
    Write-Host ""
    Write-Host "Camunda 8 ready."
    Write-Host "  REST      http://localhost:$Port"
    Write-Host "  Operate   http://localhost:$Port/operate  (demo/demo)"
    Write-Host ""
    Write-Host "  val backend = Camunda8Backend(`"http://localhost:$Port`")"
}
Write-Host ""
Write-Host "stop with: powershell -File scripts\engine.ps1 $Engine stop"
