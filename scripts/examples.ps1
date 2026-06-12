$root = Split-Path -Parent $PSScriptRoot
$lib = Join-Path $root "scripts\.lib"

$jars = @(
    "build\tasks\_loadshift-core_jarJvm\loadshift-core-jvm.jar",
    "build\tasks\_loadshift-camunda-7_jarJvm\loadshift-camunda-7-jvm.jar",
    "build\tasks\_loadshift-camunda-8_jarJvm\loadshift-camunda-8-jvm.jar"
) | ForEach-Object { Join-Path $root $_ }

foreach ($jar in $jars) {
    if (-not (Test-Path $jar)) {
        Write-Error "missing $jar - run ./amper build first"
        exit 1
    }
}

$deps = @(
    "org/camunda/bpm/model/camunda-bpmn-model/7.24.0/camunda-bpmn-model-7.24.0.jar",
    "org/camunda/bpm/model/camunda-xml-model/7.24.0/camunda-xml-model-7.24.0.jar",
    "org/jetbrains/kotlinx/kotlinx-coroutines-core-jvm/1.9.0/kotlinx-coroutines-core-jvm-1.9.0.jar",
    "org/jetbrains/kotlinx/kotlinx-datetime-jvm/0.6.1/kotlinx-datetime-jvm-0.6.1.jar",
    "org/jetbrains/kotlinx/kotlinx-serialization-json-jvm/1.7.3/kotlinx-serialization-json-jvm-1.7.3.jar",
    "org/jetbrains/kotlinx/kotlinx-serialization-core-jvm/1.7.3/kotlinx-serialization-core-jvm-1.7.3.jar"
)

New-Item -ItemType Directory -Force $lib | Out-Null
foreach ($dep in $deps) {
    $file = Join-Path $lib (Split-Path $dep -Leaf)
    if (-not (Test-Path $file)) {
        Write-Host "fetching $(Split-Path $dep -Leaf)"
        Invoke-WebRequest "https://repo1.maven.org/maven2/$dep" -OutFile $file
    }
    $jars += $file
}

$kotlinc = Get-Command kotlinc -ErrorAction SilentlyContinue
if ($kotlinc) { $kotlinc = $kotlinc.Source } else { $kotlinc = "$env:USERPROFILE\.kotlin\kotlin-2.4.0\kotlinc\bin\kotlinc.bat" }
if (-not (Test-Path $kotlinc)) {
    Write-Error "kotlinc not found - install Kotlin 2.4 (https://github.com/JetBrains/kotlin/releases) or put it on PATH"
    exit 1
}

Push-Location $root
try {
    $cp = '"' + ($jars -join ";") + '"'
    & $kotlinc -jvm-target 21 -nowarn -cp $cp -script (Join-Path $root "scripts\examples.main.kts") @args
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
