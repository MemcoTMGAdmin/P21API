#region -------------------- Globals --------------------
$EnvName        = 'PROD'
$ApiBase        = 'https://themiddletongroup-api.epicordistribution.com'
$TransactionUri = "$ApiBase/uiserver0/api/v2/transaction"

# Auth (same key as main script)
$ClientSecret  = '654cb128-ed3e-4b0a-9223-f6501b4b5052'
$GrantType     = 'client_credentials'

# Input CSV (must contain column: item_id)
$InputCsv = '.\items_to_clear.csv'
#endregion ------------------------------------------------


#region -------------------- Token acquisition --------------------
function Get-P21Token {
    param([string]$AuthBase)

    $AuthUri = "$AuthBase/api/security/token/v2"

    $Headers = @{
        Accept         = 'application/json'
        'Content-Type' = 'application/json'
    }

    $Body = @{
        ClientSecret = $ClientSecret
        GrantType    = $GrantType
    } | ConvertTo-Json -Compress

    try {
        $resp = Invoke-RestMethod -Method Post -Uri $AuthUri -Headers $Headers -Body $Body
        foreach ($p in 'AccessToken','access_token','token') {
            if ($resp.PSObject.Properties.Name -contains $p) {
                return $resp.$p
            }
        }
        throw "Unexpected token response shape. Properties: $($resp.PSObject.Properties.Name -join ', ')"
    }
    catch {
        # Best-effort: show HTTP body if present
        $httpBody = $null
        try {
            if ($_.Exception.Response) {
                $stream = $_.Exception.Response.GetResponseStream()
                if ($stream) {
                    $reader = New-Object System.IO.StreamReader($stream)
                    $httpBody = $reader.ReadToEnd()
                    $reader.Close()
                }
            }
        } catch {}

        throw "Failed to acquire token: $($_.Exception.Message)`nResponseBody: $httpBody"
    }
}
#endregion ------------------------------------------------


#region -------------------- Request body builder --------------------
function New-ClearClassId2Body {
    param([Parameter(Mandatory)][string]$ItemId)

    $body = @{
        Name           = 'Item'
        UseCodeValues  = $false
        IgnoreDisabled = $false
        Transactions   = @(
            @{
                Status = 'New'
                DataElements = @(
                    @{
                        Name = 'TABPAGE_1.tp_1_dw_1'
                        Type = 'Form'
                        Keys = @('item_id')
                        Rows = @(
                            @{
                                Edits = @(
                                    @{ Name = 'item_id'; Value = $ItemId; IgnoreIfEmpty = $true }
                                )
                                RelativeDateEdits = @()
                            }
                        )
                    },
                    @{
                        Name = 'TABPAGE_CLASSES.classes'
                        Type = 'Form'
                        Keys = @()
                        Rows = @(
                            @{
                                Edits = @(
                                    @{ Name = 'class_id2'; Value = 'Seasonal Item'; IgnoreIfEmpty = $false }
                                )
                                RelativeDateEdits = @()
                            }
                        )
                    }
                )
                Documents = $null
            }
        )
    }

    $body | ConvertTo-Json -Depth 12 -Compress
}
#endregion ------------------------------------------------

#region -------------------- Main --------------------
Write-Host "Environment: $EnvName" -ForegroundColor Cyan

$RunTs   = (Get-Date).ToString('yyyyMMdd_HHmmss')
$OutDir  = Join-Path (Get-Location) 'out'
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
$CsvOut  = Join-Path $OutDir "clear_class_id2_summary_$RunTs.csv"

$SummaryRows = @()

# Load CSV
if (-not (Test-Path $InputCsv)) { throw "Input CSV not found: $InputCsv" }

$items = Import-Csv $InputCsv
if (-not $items -or $items.Count -eq 0) { throw "CSV is empty: $InputCsv" }

$first = $items | Select-Object -First 1
if (-not ($first.PSObject.Properties.Name -contains 'item_id')) {
    throw "CSV must contain column: item_id"
}

# Normalize + validate (PowerShell 5.1-safe)
$itemIds = @()
foreach ($row in $items) {
    $v = $row.item_id
    if ($null -ne $v) {
        $s = $v.ToString().Trim()
        if (-not [string]::IsNullOrWhiteSpace($s)) { $itemIds += $s }
    }
}
if ($itemIds.Count -eq 0) { throw "No valid item_id values found in CSV" }

# Auth
$token = Get-P21Token -AuthBase $ApiBase

$Headers = @{
    Authorization  = "Bearer $token"
    Accept         = 'application/json'
    'Content-Type' = 'application/json'
}

Write-Host "Items to process: $($itemIds.Count)" -ForegroundColor Cyan
Write-Host "Posting to: $TransactionUri" -ForegroundColor DarkCyan
Write-Host ""

foreach ($itemId in $itemIds) {
    Write-Host "Processing item_id = $itemId" -ForegroundColor Yellow

    $body = New-ClearClassId2Body -ItemId $itemId

    try {
        $resp = Invoke-RestMethod -Method Post -Uri $TransactionUri -Headers $Headers -Body $body

        # Determine transaction-level success (2xx can still be Failed)
        $txStatus = $null
        $messages = ''
        $ok       = $false

        try {
            $txStatus = $resp.Results.Transactions[0].Status
            if ($resp.Messages) { $messages = ($resp.Messages -join ' | ') }
            $ok = ($resp.Summary.Succeeded -gt 0 -and $resp.Summary.Failed -eq 0)
        } catch {
            $ok = $false
        }

        $SummaryRows += [pscustomobject]@{
            item_id  = $itemId
            success  = $ok
            txStatus = $txStatus
            message  = $messages
        }
    }
    catch {
        # Transport/HTTP failure (non-2xx)
        $SummaryRows += [pscustomobject]@{
            item_id  = $itemId
            success  = $false
            txStatus = 'HTTP_FAIL'
            message  = $_.Exception.Message
        }

        Write-Host "HTTP FAIL for item_id $itemId" -ForegroundColor Red
    }
}

$SummaryRows | Export-Csv -Path $CsvOut -NoTypeInformation -Encoding UTF8
Write-Host "Summary CSV: $CsvOut" -ForegroundColor Cyan
Write-Host "Done. Items processed: $($itemIds.Count)" -ForegroundColor Green
#endregion ------------------------------------------------
