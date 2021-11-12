$versions = @("3.0.1", "3.0.2", "3.0.3", "3.1.1", "3.1.2", "3.2.0")
$jarPath = "./target/jars"

Write-Host "Clearing existing jar artefacts" -ForegroundColor Green
if (Test-Path $jarPath) {
    Remove-Item -Path $jarPath -Force -Recurse
}

New-Item -Path $jarPath -ItemType Directory

foreach ($version in $versions) {
    Write-Host "Building for Spark version: $version" -ForegroundColor Green
    & sbt -DsparkVersion="$version" clean compile test package
}

Write-Host "Copying jar files to $jarPath" -ForegroundColor Green
Get-ChildItem -Filter "spark-cef*.jar" -Path ./target -Recurse | Copy-Item -Destination $jarPath
