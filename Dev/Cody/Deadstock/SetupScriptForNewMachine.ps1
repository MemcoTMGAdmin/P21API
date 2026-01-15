# Creates DPAPI-encrypted secret blobs tied to *this* user + machine.
# Run under the SAME account that will execute the job (service account).

$ErrorActionPreference = 'Stop'

function Write-SecretFile {
    param(
        [Parameter(Mandatory=$true)][string]$Path,
        [Parameter(Mandatory=$true)][string]$Prompt
    )

    $sec = Read-Host $Prompt -AsSecureString
    $sec | ConvertFrom-SecureString | Set-Content -Path $Path -Encoding UTF8
    Write-Host "Wrote: $Path"
}

Write-SecretFile -Path ".\sqlpwd.txt"     -Prompt "Enter SQL password"
Write-SecretFile -Path ".\smtp2go_pwd.txt" -Prompt "Enter SMTP2GO password"

Write-Host "`nDone. These files are only usable by this Windows account on this machine."
