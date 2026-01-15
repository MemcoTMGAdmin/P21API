#region -------------------- Parameters & Globals --------------------
param(
    [Parameter(Mandatory = $false)]
    [ValidateSet('New','Update')]
    [string]$TransactionStatus = 'New',   # BE v2 upsert path: New updates when key exists

    [Parameter(Mandatory = $false)]
    [switch]$ShowProgress = $true,

    [Parameter(Mandatory = $false)]
    [int]$ProgressEvery = 25              # update progress every N rows
)

$ProgressPreference = 'SilentlyContinue'

# --- Environment toggles ---
$EnvName           = 'DEV'  # 'DEV' | 'PROD' (informational)
$P21ApiBase        = 'https://themiddletongroup-play-api.epicordistribution.com'
$TransactionUri    = "$P21ApiBase/uiserver0/api/v2/transaction"
$ApiMaxAttempts = 4
$ApiBaseDelayMs = 1000
$ApiMaxDelayMs  = 8000
$ApiTimeoutSec  = 120

# --- SQL connection info (SQL auth ONLY) ---
$SqlServer         = 'p21us-read06.epicordistribution.com,50135'
$SqlDatabase       = 'az_130611_live'
$SqlUser           = 'readonly_130611_live'
$SqlPassword = Get-Content .\sqlpwd.txt | ConvertTo-SecureString

# --- T-SQL: MUST RETURN columns [item_id], [status]; extras are fine ---
$Tsql = @'
DECLARE @today_utc date = CAST(SYSUTCDATETIME() AS date);
DECLARE @d45       date = DATEADD(DAY, -45, @today_utc);
DECLARE @d90       date = DATEADD(DAY, -90, @today_utc);

WITH qty_by_item AS (
    SELECT
        im.item_id,
        COALESCE(SUM(iloc.qty_on_hand), 0) AS qty
    FROM dbo.inv_mast im
    LEFT JOIN dbo.inv_loc iloc
        ON iloc.inv_mast_uid = im.inv_mast_uid
    GROUP BY
        im.item_id
),
last_invoice_by_item AS (
    SELECT
        im.item_id,
        MAX(il.date_created) AS last_invoiced
    FROM dbo.inv_mast im
    LEFT JOIN dbo.invoice_line il
        ON il.inv_mast_uid = im.inv_mast_uid
    GROUP BY
        im.item_id
),
first_receipt_by_item AS (
    SELECT
        im.item_id,
        MIN(CASE WHEN pl.received_date IS NOT NULL THEN pl.received_date END) AS first_received
    FROM dbo.inv_mast im
    LEFT JOIN dbo.po_line pl
        ON pl.inv_mast_uid = im.inv_mast_uid
    GROUP BY
        im.item_id
)
SELECT TOP (1)
    im.item_id,
    im.inv_mast_uid,
    q.qty,
    li.last_invoiced,
    im.class_id2,
    fr.first_received,
    CASE
        /* 1) Seasonal override (human-curated wins, normalize to allowed label) */
        WHEN im.class_id2 IS NOT NULL
         AND LOWER(im.class_id2) LIKE '%seas%' THEN 'Seasonal Item'

        /* 2) No stock on hand */
        WHEN q.qty = 0 THEN NULL

        /* 3) Was DEAD, but sold within last 90 days -> still Dead Stock (keep branch for future) */
        WHEN im.class_id2 IS NOT NULL
         AND LOWER(im.class_id2) LIKE '%dead%'
         AND li.last_invoiced IS NOT NULL
         AND CAST(li.last_invoiced AS date) >= @d90 THEN 'Dead Stock'

        /* 4) Dead Stock (independent of current class), stocked & stale */
        WHEN q.qty > 0 AND (
               (li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) < @d90)
            OR (li.last_invoiced IS NULL     AND fr.first_received IS NOT NULL AND CAST(fr.first_received AS date) < @d90)
        ) THEN 'Dead Stock'

        /* 5) 45 Day Inactive Item: 46–90 day quiet window, only if currently unclassified */
        WHEN q.qty > 0
         AND im.class_id2 IS NULL
         AND li.last_invoiced IS NOT NULL
         AND CAST(li.last_invoiced AS date) <  @d45
         AND CAST(li.last_invoiced AS date) >= @d90 THEN '45 Day Inactive Item'

        /* 6) Otherwise active/normal */
        ELSE NULL
    END AS status
FROM dbo.inv_mast im
LEFT JOIN qty_by_item           q  ON q.item_id  = im.item_id
LEFT JOIN last_invoice_by_item  li ON li.item_id = im.item_id
LEFT JOIN first_receipt_by_item fr ON fr.item_id = im.item_id

/* ---- Return only rows that actually need updating ----*/
WHERE NOT (
    (im.class_id2 IS NULL AND
     CASE
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%seas%' THEN 'Seasonal Item'
        WHEN q.qty = 0 THEN NULL
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%dead%' AND li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) >= @d90 THEN 'Dead Stock'
        WHEN q.qty > 0 AND (
               (li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) < @d90)
            OR (li.last_invoiced IS NULL     AND fr.first_received IS NOT NULL AND CAST(fr.first_received AS date) < @d90)
        ) THEN 'Dead Stock'
        WHEN q.qty > 0 AND im.class_id2 IS NULL AND li.last_invoiced IS NOT NULL
             AND CAST(li.last_invoiced AS date) < @d45 AND CAST(li.last_invoiced AS date) >= @d90 THEN '45 Day Inactive Item'
        ELSE NULL
     END IS NULL)
 OR
    (im.class_id2 IS NOT NULL AND
     im.class_id2 = CASE
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%seas%' THEN 'Seasonal Item'
        WHEN q.qty = 0 THEN NULL
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%dead%' AND li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) >= @d90 THEN 'Dead Stock'
        WHEN q.qty > 0 AND (
               (li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) < @d90)
            OR (li.last_invoiced IS NULL     AND fr.first_received IS NOT NULL AND CAST(fr.first_received AS date) < @d90)
        ) THEN 'Dead Stock'
        WHEN q.qty > 0 AND im.class_id2 IS NULL AND li.last_invoiced IS NOT NULL
             AND CAST(li.last_invoiced AS date) < @d45 AND CAST(li.last_invoiced AS date) >= @d90 THEN '45 Day Inactive Item'
        ELSE NULL
     END)
)
ORDER BY
    im.item_id;
'@



#region -------------------- Token acquisition ----------------------
function Get-P21Token {
    param(
        [Parameter(Mandatory=$false)][string]$AuthBase = "https://themiddletongroup-play-api.epicordistribution.com"
    )

    $AuthUri = "$AuthBase/api/security/token/v2"
    $ClientSecret = "c74ec0f8-220e-4203-a350-051a2bbe0bf4"  # replace before prod if needed
    $GrantType = "client_credentials"

    $Headers = @{
        "Accept" = "application/json"
        "Content-Type" = "application/json"
    }

    $Body = @{
        "ClientSecret" = $ClientSecret
        "GrantType"    = $GrantType
    } | ConvertTo-Json -Compress

    try {
        $response = Invoke-RestMethod -Uri $AuthUri -Method Post -Headers $Headers -Body $Body -TimeoutSec 60
        $props = $response.PSObject.Properties.Name
        if     ($props -contains 'AccessToken') { return $response.AccessToken }
        elseif ($props -contains 'access_token') { return $response.access_token }
        elseif ($props -contains 'token')        { return $response.token }
        else { throw "Unexpected token response format: $($response | ConvertTo-Json -Depth 5)" }
    } catch {
        throw "Failed to get Prophet21 token: $($_.Exception.Message)"
    }
}
#endregion -----------------------------------------------------------

#region -------------------- SQL data access (SQL auth only) --------
function Invoke-SqlQuery {
    param(
        [Parameter(Mandatory=$true)][string]$Server,
        [Parameter(Mandatory=$true)][string]$Database,
        [Parameter(Mandatory=$true)][string]$Query,
        [Parameter(Mandatory=$true)][string]$UserName,
        [Parameter(Mandatory=$true)][System.Security.SecureString]$Password
    )

    $pwdPtr   = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Password)
    $plainPwd = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pwdPtr)
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pwdPtr)

    $connString = "Server=$Server;Database=$Database;User ID=$UserName;Password=$plainPwd;TrustServerCertificate=True;"
    $table = New-Object System.Data.DataTable
    $conn  = New-Object System.Data.SqlClient.SqlConnection $connString
    $cmd   = $conn.CreateCommand()
    $cmd.CommandText = $Query
    $cmd.CommandTimeout = 600  # seconds

    try {
        $conn.Open()
        $reader = $cmd.ExecuteReader()
        $table.Load($reader)
        $reader.Close()
        return ,$table
    }
    finally {
        $plainPwd = $null
        $conn.Close()
        $conn.Dispose()
    }
}
#endregion -----------------------------------------------------------

#region -------------------- API helpers ----------------------------
function Invoke-RestMethodWithRetry {
    param(
        [Parameter(Mandatory=$true)][string]$Uri,
        [Parameter(Mandatory=$true)][hashtable]$Headers,
        [Parameter(Mandatory=$true)][string]$Body,
        [int]$TimeoutSec = 120,
        [int]$MaxAttempts = 4,
        [int]$BaseDelayMs = 1000,
        [int]$MaxDelayMs  = 8000
    )

    $attempt = 0
    $lastStatus = $null

    while ($true) {
        $attempt++
        try {
            $resp = Invoke-RestMethod -Uri $Uri -Method Post -Headers $Headers -Body $Body -TimeoutSec $TimeoutSec -MaximumRedirection 0
            return [pscustomobject]@{ Response = $resp; Attempts = $attempt; HttpStatus = 200 }
        }
        catch {
            # Try to extract HTTP status code (if present)
            $statusCode = $null
            try {
                if ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
                    # Works in many Windows PowerShell cases
                    $statusCode = [int]$_.Exception.Response.StatusCode.value__
                }
            } catch { }

            $lastStatus = $statusCode

            $retryable = $false
            if ($null -eq $statusCode) {
                # No HTTP response -> likely transient network/timeout
                $retryable = $true
            } elseif ($statusCode -in 408,429,500,502,503,504) {
                $retryable = $true
            }

            if (-not $retryable -or $attempt -ge $MaxAttempts) {
                # Re-throw with context
                $msg = $_.Exception.Message
                if ($statusCode) { $msg = "HTTP $(statusCode): $msg" }
                throw "API call failed after $attempt attempt(s). $msg"
            }

            # Exponential backoff + jitter (and optional Retry-After for 429)
            $delayMs = [math]::Min($MaxDelayMs, $BaseDelayMs * [math]::Pow(2, $attempt - 2))
            $jitter  = Get-Random -Minimum 0 -Maximum 250
            $delayMs = [int]($delayMs + $jitter)

            Start-Sleep -Milliseconds $delayMs
        }
    }
}

function New-P21ItemStatusBody {
    param(
        [Parameter(Mandatory=$true)][string]$ItemId,
        [Parameter(Mandatory=$true)][AllowNull()][string]$NewStatus,
        [Parameter(Mandatory=$true)][ValidateSet('New','Update')]$TransactionStatus
    )

    $body = @{
        Name                   = 'Item'
        UseCodeValues          = $false
        IgnoreDisabled         = $false
        Transactions           = @(
            @{
                Status        = $TransactionStatus
                DataElements  = @(
                    @{
                        Name               = 'TABPAGE_1.tp_1_dw_1'
                        BusinessObjectName = $null
                        Type               = 'Form'
                        Keys               = @('item_id')
                        Rows               = @(
                            @{
                                Edits = @(
                                    @{ Name='item_id'; Value=$ItemId; IgnoreIfEmpty=$true }
                                )
                                RelativeDateEdits = @()
                            }
                        )
                    },
                    @{
                        Name               = 'TABPAGE_CLASSES.classes'
                        BusinessObjectName = $null
                        Type               = 'Form'
                        Keys               = @('item_id')             
                        Rows               = @(
                            @{
                                Edits = @(
                                    @{ Name='item_id';  Value=$ItemId;    IgnoreIfEmpty=$true },    # echo key
                                    @{ Name='class_id2'; Value=$NewStatus; IgnoreIfEmpty=$false }   # can be $null to clear
                                )
                                RelativeDateEdits = @()
                            }
                        )
                    }
                )
                Documents     = $null
            }
        )
        Query                  = $null
        FieldMap               = @()
        TransactionSplitMethod = 0
        Parameters             = $null
    }

    ($body | ConvertTo-Json -Depth 12 -Compress)
}
#endregion -----------------------------------------------------------

#region -------------------- Main flow --------------------------------
# --- Logging ---
$ts        = (Get-Date).ToString('yyyyMMdd_HHmmss')
$LogDir    = Join-Path -Path (Get-Location) -ChildPath 'logs'
$OkLog     = Join-Path $LogDir "p21_status_updates_ok_$ts.csv"
$ErrLog    = Join-Path $LogDir "p21_status_updates_err_$ts.csv"
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

Write-Host "Environment: $EnvName  TxStatus: $TransactionStatus" -ForegroundColor Cyan

# 1) Get token
$Token = Get-P21Token

# 2) Pull dataset
$data = Invoke-SqlQuery -Server $SqlServer -Database $SqlDatabase -Query $Tsql -UserName $SqlUser -Password $SqlPassword
if (-not $data -or $data.GetType().FullName -ne 'System.Data.DataTable') {
    throw "Expected System.Data.DataTable; got: $($data.GetType().FullName)"
}
if ($data.Rows.Count -eq 0) {
    Write-Host "No rows returned from SQL. Nothing to do." -ForegroundColor Yellow
    return
}

# 3) Validate shape
foreach ($col in @('item_id','status')) {
    if (-not $data.Columns.Contains($col)) { throw "SQL result is missing required column: $col" }
}

# 3c) Ensure class_id2 column is present (we want it in logs)
if (-not $data.Columns.Contains('class_id2')) {
    throw "SQL result is missing expected column for logging: class_id2"
}

# 3a) Validate item_id non-empty after ToString().Trim()
$badIdRows = @()
foreach ($r in $data.Rows) {
    $iid = if ($null -ne $r['item_id']) { ($r['item_id'].ToString()).Trim() } else { '' }
    if ([string]::IsNullOrWhiteSpace($iid)) {
        $badIdRows += [pscustomobject]@{
            item_id = $r['item_id']
            status  = [string]$r['status']
            reason  = 'empty_item_id_after_trim'
        }
    }
}
if ($badIdRows.Count -gt 0) {
    $badPath = Join-Path $LogDir "rejected_empty_itemid_$ts.csv"
    $badIdRows | Export-Csv -NoTypeInformation -Path $badPath
    throw "Aborting: $($badIdRows.Count) row(s) with empty/whitespace item_id. See $badPath"
}

# 3b) Validate statuses (allow null or 'seas' variants; else DEAD/UNDEAD/INACTIVE)
$badRows = @()
foreach ($row in $data.Rows) {
    $s = $row['status']
    if ($null -eq $s) { continue }
    $sText = $s.ToString()
    if ($sText -match '(?i)seas') { continue }
}
if ($badRows.Count -gt 0) {
    $badPath = Join-Path $LogDir "rejected_rows_$ts.csv"
    $badRows | Export-Csv -NoTypeInformation -Path $badPath
    throw "Aborting: found $($badRows.Count) row(s) with invalid status. See $badPath"
}

# 4) Prepare headers
$Headers = @{
    'Authorization' = "Bearer $Token"
    'Content-Type'  = 'application/json'
    'Accept'        = 'application/json'
}

# 5) Process rows (serial with progress)
$ok   = @()
$errb = @()

$rows  = @($data.Rows)
$total = $rows.Count
$sw = [System.Diagnostics.Stopwatch]::StartNew()

for ($i = 0; $i -lt $total; $i++) {
    $r = $rows[$i]
    try {
        $itemIdObj = $r['item_id']
        $itemId    = if ($null -ne $itemIdObj) { ($itemIdObj.ToString()).Trim() } else { '' }

        $statusObj = $r['status']
        $newStatus = if ($null -ne $statusObj) { ($statusObj.ToString()).ToUpperInvariant() } else { $null }

        $class2Obj   = $r['class_id2']
        $origClass2  = if ($null -ne $class2Obj) { ($class2Obj.ToString()).Trim() } else { $null }

        if ([string]::IsNullOrWhiteSpace($itemId)) { throw "empty item_id after ToString()+Trim()" }

        $jsonBody  = New-P21ItemStatusBody -ItemId $itemId -NewStatus $newStatus -TransactionStatus $TransactionStatus

        $call = Invoke-RestMethodWithRetry -Uri $TransactionUri -Headers $Headers -Body $jsonBody `
            -TimeoutSec $ApiTimeoutSec -MaxAttempts $ApiMaxAttempts -BaseDelayMs $ApiBaseDelayMs -MaxDelayMs $ApiMaxDelayMs

        $resp     = $call.Response
        $attempts = $call.Attempts
        $respId   = $null
        if ($resp -and $resp.PSObject.Properties.Name -contains 'id') { $respId = $resp.id }

        $ok += [pscustomobject]@{
            item_id          = $itemId
            originalClassId2 = $origClass2
            requestedStatus  = $newStatus
            httpStatus       = 200
            responseId       = $respId
            attempts         = $attempts
        }
     } catch {
        $errb += [pscustomobject]@{
            item_id          = [string]$r['item_id']
            originalClassId2 = [string]$r['class_id2']
            requestedStatus  = [string]$r['status']
            error            = $_.Exception.Message
        }
        # try to extract HTTP response body/status when available
        $respObj = $null
        try {
            if ($_.Exception.Response) {
                $stream = $_.Exception.Response.GetResponseStream()
                if ($stream) {
                    $reader = New-Object System.IO.StreamReader($stream)
                    $body = $reader.ReadToEnd()
                    $reader.Close()
                    $statusCode = $null
                    try { $statusCode = [int]$_.Exception.Response.StatusCode.value__ } catch {}
                    $respObj = @{ StatusCode = $statusCode; Body = $body }
                }
            }
        } catch {
            $respObj = @{ Info = "Could not extract response"; Msg = $_.Exception.Message }
        }

        $errb += [pscustomobject]@{
            item_id          = [string]$r['item_id']
            originalClassId2 = [string]$r['class_id2']
            requestedStatus  = [string]$r['status']
            error            = $_.Exception.Message
            response         = $respObj
        }
    }
    if ($ShowProgress -and (($i + 1) % $ProgressEvery -eq 0 -or $i -eq 0 -or $i -eq $total - 1)) {
        $pct      = [int]((($i + 1) / [double]$total) * 100)
        $elapsed  = $sw.Elapsed
        $rate     = ($i + 1) / [math]::Max($elapsed.TotalSeconds, 0.001)
        $remainS  = ($total - ($i + 1)) / $rate
        $eta      = [TimeSpan]::FromSeconds([math]::Max($remainS, 0))
        Write-Progress -Activity "Updating P21 item status (BE v2, Status=$TransactionStatus)" `
                       -Status   ("{0}/{1} | OK:{2} ERR:{3} | Elapsed {4:mm\:ss} | ETA {5:mm\:ss}" -f ($i+1), $total, $ok.Count, $errb.Count, $elapsed, $eta) `
                       -PercentComplete $pct
    }
}
if ($ShowProgress) { Write-Progress -Activity "Updating P21 item status" -Completed }

# 6) Write logs
$ts2      = (Get-Date).ToString('yyyyMMdd_HHmmss')
$OkLog2   = Join-Path $LogDir "p21_status_updates_ok_$ts2.csv"
$ErrLog2  = Join-Path $LogDir "p21_status_updates_err_$ts2.csv"
$ok  | Export-Csv -NoTypeInformation -Path $OkLog2
$errb| Export-Csv -NoTypeInformation -Path $ErrLog2

Write-Host ("Done. OK: {0}  ERR: {1}" -f $ok.Count, $errb.Count) -ForegroundColor Green
Write-Host "Logs: `n  $OkLog2`n  $ErrLog2"
# 6) Write JSON logs (includes full REST response objects)
$ts2      = (Get-Date).ToString('yyyyMMdd_HHmmss')
$OkLog2   = Join-Path $LogDir "p21_status_updates_ok_$ts2.json"
$ErrLog2  = Join-Path $LogDir "p21_status_updates_err_$ts2.json"

# pretty-print JSON with sufficient depth to include nested REST responses
$ok  | ConvertTo-Json -Depth 12 | Set-Content -Path $OkLog2 -Encoding UTF8
$errb| ConvertTo-Json -Depth 12 | Set-Content -Path $ErrLog2 -Encoding UTF8

Write-Host ("Done. OK: {0}  ERR: {1}" -f $ok.Count, $errb.Count) -ForegroundColor Green
Write-Host "Logs: `n  $OkLog2`n  $ErrLog2"
#endregion -----------------------------------------------------------