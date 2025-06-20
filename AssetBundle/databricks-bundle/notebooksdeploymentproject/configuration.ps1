# PowerShell script to configure multiple Databricks CLI profiles

$homeDir = $env:USERPROFILE
$cfgPath = "$homeDir\.databrickscfg"

# Prompt user for tokens
$devToken = Read-Host "yourtoken"
$testToken = Read-Host "yourtoken"

# Define workspace URLs (replace with yours)
$devHost = "https://adb-2058598773075952.12.azuredatabricks.net/"
$testHost = "https://adb-3378109637561353.13.azuredatabricks.net/"


# Create content for the config file
$configContent = @"
[dev]
host = $devHost
token = $devToken

[test]
host = $testHost
token = $testToken


"@

# Write to ~/.databrickscfg
$configContent | Out-File -Encoding ASCII -FilePath $cfgPath -Force

Write-Host "`n✅ Databricks CLI profiles configured at $cfgPath" -ForegroundColor Green
