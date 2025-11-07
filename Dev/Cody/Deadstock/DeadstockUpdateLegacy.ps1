<#  Update P21 item status (class_id2) from SQL result set via API
    PS5.1-friendly (serial only; no -Parallel)
    - DryRun supported
    - CSV logs written to ./logs/
    - Note: -Throttle is ignored in this version
#>

#region -------------------- Parameters & Globals --------------------
param(
    [Parameter(Mandatory = $false)]
    [switch]$DryRun,

    [Parameter(Mandatory = $false)]
    [int]$Throttle = 1,                 # Ignored in PS5 version (serial only)

    [Parameter(Mandatory = $false)]
    [ValidateSet('New','Update')]
    [string]$TransactionStatus = 'Update'   # flip if your endpoint insists on "New"
)

$ProgressPreference = 'SilentlyContinue'


$EnvName           = 'DEV'  # 'DEV' | 'PROD' (informational)
$P21ApiBase        = 'https://themiddletongroup-play-api.epicordistribution.com'
$TransactionUri    = "$P21ApiBase/uiserver0/api/v2/transaction"


$SqlServer         = 'p21us-read06.epicordistribution.com,50135'
$SqlDatabase       = 'az_130611_live'
$SqlUser           = 'readonly_130611_live'
$SqlPassword       = 'TWbrLcyz!5s69ab5p' 


$Tsql = @'
/* === Anchors for day-granularity windows (avoid TZ/off-by-one flaps) === */
DECLARE @today_utc date = CAST(SYSUTCDATETIME() AS date);
DECLARE @d45  date = DATEADD(DAY, -45, @today_utc);
DECLARE @d90  date = DATEADD(DAY, -90, @today_utc);

/* === Pre-aggregations to kill fan-out === */
WITH qty_by_item AS (
    SELECT im.item_id, COALESCE(SUM(iloc.qty_on_hand), 0) AS qty
    FROM dbo.inv_mast im
    LEFT JOIN dbo.inv_loc iloc ON iloc.inv_mast_uid = im.inv_mast_uid
    GROUP BY im.item_id
),
last_invoice_by_item AS (
    SELECT im.item_id, MAX(il.date_created) AS last_invoiced
    FROM dbo.inv_mast im
    LEFT JOIN dbo.invoice_line il ON il.inv_mast_uid = im.inv_mast_uid
    GROUP BY im.item_id
),
first_receipt_by_item AS (
    SELECT im.item_id,
           MIN(CASE WHEN pl.received_date IS NOT NULL THEN pl.received_date END) AS first_received
    FROM dbo.inv_mast im
    LEFT JOIN dbo.po_line pl ON pl.inv_mast_uid = im.inv_mast_uid
    GROUP BY im.item_id
)
/* === Final classification with explicit precedence === */
SELECT
    im.item_id,
    q.qty,
    li.last_invoiced,
    im.class_id2,
    fr.first_received,
    CASE
        /* 1) Seasonal override (human-curated wins) */
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%seas%' THEN im.class_id2

        /* 2) No stock on hand */
        WHEN q.qty = 0 THEN NULL

        /* 3) Was DEAD, but sold within last 90 days -> UNDEAD */
        WHEN im.class_id2 IS NOT NULL
         AND LOWER(im.class_id2) LIKE '%dead%'
         AND li.last_invoiced IS NOT NULL
         AND CAST(li.last_invoiced AS date) >= @d90 THEN 'UNDEAD'

        /* 4) DEAD (independent of current class), stocked & stale */
        WHEN q.qty > 0 AND (
               (li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) < @d90)
            OR (li.last_invoiced IS NULL     AND fr.first_received IS NOT NULL AND CAST(fr.first_received AS date) < @d90)
        ) THEN 'DEAD'

        /* 5) INACTIVE: 46–90 day quiet window, only if currently unclassified */
        WHEN q.qty > 0
         AND im.class_id2 IS NULL
         AND li.last_invoiced IS NOT NULL
         AND CAST(li.last_invoiced AS date) <  @d45
         AND CAST(li.last_invoiced AS date) >= @d90 THEN 'INACTIVE'

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
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%seas%' THEN im.class_id2
        WHEN q.qty = 0 THEN NULL
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%dead%' AND li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) >= @d90 THEN 'UNDEAD'
        WHEN q.qty > 0 AND (
               (li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) < @d90)
            OR (li.last_invoiced IS NULL     AND fr.first_received IS NOT NULL AND CAST(fr.first_received AS date) < @d90)
        ) THEN 'DEAD'
        WHEN q.qty > 0 AND im.class_id2 IS NULL AND li.last_invoiced IS NOT NULL
             AND CAST(li.last_invoiced AS date) < @d45 AND CAST(li.last_invoiced AS date) >= @d90 THEN 'INACTIVE'
        ELSE NULL
     END IS NULL)
 OR
    (im.class_id2 IS NOT NULL AND
     im.class_id2 = CASE
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%seas%' THEN im.class_id2
        WHEN q.qty = 0 THEN NULL
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%dead%' AND li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) >= @d90 THEN 'UNDEAD'
        WHEN q.qty > 0 AND (
               (li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) < @d90)
            OR (li.last_invoiced IS NULL     AND fr.first_received IS NOT NULL AND CAST(fr.first_received AS date) < @d90)
        ) THEN 'DEAD'
        WHEN q.qty > 0 AND im.class_id2 IS NULL AND li.last_invoiced IS NOT NULL
             AND CAST(li.last_invoiced AS date) < @d45 AND CAST(li.last_invoiced AS date) >= @d90 THEN 'INACTIVE'
        ELSE NULL
     END)
);
'@

# --- Allowed statuses (guardrail) ---
# We accept: $null, any seasonal-like string, or strict DEAD/UNDEAD/INACTIVE
$AllowedStatuses = 'DEAD','UNDEAD','INACTIVE'
#endregion -----------------------------------------------------------

#region -------------------- Token acquisition ----------------------
function Get-P21Token {
    param(
        [Parameter(Mandatory=$false)][string]$AuthBase = "https://themiddletongroup-play-api.epicordistribution.com"
    )

    $AuthUri = "$AuthBase/api/security/token/v2"
    $ClientSecret = "c74ec0f8-220e-4203-a350-051a2bbe0bf4"  # <-- real one
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

        # Normalize property bag (handles PascalCase / camelCase variants)
        $props = $response.PSObject.Properties.Name

        if ($props -contains 'AccessToken') { return $response.AccessToken }
        if ($props -contains 'access_token') { return $response.access_token }
        if ($props -contains 'token')        { return $response.token }

        throw "Unexpected token response format: $($response | ConvertTo-Json -Depth 5)"
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
        [Parameter(Mandatory=$true)][string]$Password
    )

    $connString = "Server=$Server;Database=$Database;User ID=$UserName;Password=$Password;TrustServerCertificate=True;"
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
        return $table
    }
    finally {
        $conn.Close()
        $conn.Dispose()
    }
}
#endregion -----------------------------------------------------------

#region -------------------- API helpers ----------------------------
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
                                    @{
                                        Name          = 'item_id'
                                        Value         = $ItemId
                                        IgnoreIfEmpty = $true
                                    }
                                )
                                RelativeDateEdits = @()
                            }
                        )
                    },
                    @{
                        Name               = 'TABPAGE_CLASSES.classes'
                        BusinessObjectName = $null
                        Type               = 'Form'
                        Keys               = @()
                        Rows               = @(
                            @{
                                Edits = @(
                                    @{
                                        Name          = 'class_id2'
                                        Value         = $NewStatus    # will serialize to null if $null
                                        IgnoreIfEmpty = $false
                                    }
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

if ($Throttle -gt 1) {
    Write-Warning "PS5 version runs serially. -Throttle is ignored."
}

Write-Host "Environment: $EnvName  DryRun: $DryRun  TxStatus: $TransactionStatus" -ForegroundColor Cyan

# 1) Get token
$Token = Get-P21Token

# 2) Pull dataset
$data = Invoke-SqlQuery -Server $SqlServer -Database $SqlDatabase -Query $Tsql -UserName $SqlUser -Password $SqlPassword

if (-not $data -or $data.Rows.Count -eq 0) {
    Write-Host "No rows returned from SQL. Nothing to do." -ForegroundColor Yellow
    return
}

# 3) Validate shape & statuses
foreach ($col in @('item_id','status')) {
    if (-not $data.Columns.Contains($col)) {
        throw "SQL result is missing required column: $col"
    }
}

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
if ($badRows.Count -gt 0) {
    $badPath = Join-Path $LogDir "rejected_rows_$ts.csv"
    $badRows | Export-Csv -NoTypeInformation -Path $badPath
    throw "Aborting: found $($badRows.Count) row(s) with invalid status. See $badPath"
}

# 4) Prepare headers
$Headers = @{
    'Content-Type'  = 'application/json'
    'Authorization' = "Bearer $Token"
}

# 5) Process rows (serial only)
$ok   = @()
$errb = @()

foreach ($r in $data.Rows) {
    try {
        $itemIdObj = $r['item_id']
        $itemId    = if ($null -ne $itemIdObj) { ($itemIdObj.ToString()).Trim() } else { '' }
        # Normalize to upper if not null; leave $null as-is to clear class_id2
        $newStatus = if ($null -ne $r['status']) { ($r['status'].ToString()).ToUpperInvariant() } else { $null }
        $jsonBody  = New-P21ItemStatusBody -ItemId $itemId -NewStatus $newStatus -TransactionStatus $TransactionStatus

        if ($DryRun) {
            $ok += New-Object psobject -Property @{
                item_id         = $itemId
                requestedStatus = $newStatus
                httpStatus      = 'DRYRUN'
                responseId      = $null
            }
            continue
        }

        $resp = Invoke-RestMethod -Uri $TransactionUri -Method Post -Headers $Headers -Body $jsonBody -TimeoutSec 120 -MaximumRedirection 0

        $respId = $null
        if ($resp -and $resp.PSObject.Properties.Name -contains 'id') { $respId = $resp.id }

        $ok += New-Object psobject -Property @{
            item_id         = $itemId
            requestedStatus = $newStatus
            httpStatus      = 200
            responseId      = $respId
        }
    } catch {
        $errb += New-Object psobject -Property @{
            item_id         = [string]$r.item_id
            requestedStatus = [string]$r.status
            error           = $_.Exception.Message
        }
    }
}

# 6) Write logs
$ts        = (Get-Date).ToString('yyyyMMdd_HHmmss')  # refresh in case long run
$LogDir    = Join-Path -Path (Get-Location) -ChildPath 'logs'
$OkLog     = Join-Path $LogDir "p21_status_updates_ok_$ts.csv"
$ErrLog    = Join-Path $LogDir "p21_status_updates_err_$ts.csv"
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

$ok  | Export-Csv -NoTypeInformation -Path $OkLog
$errb| Export-Csv -NoTypeInformation -Path $ErrLog

Write-Host ("Done. OK: {0}  ERR: {1}" -f $ok.Count, $errb.Count) -ForegroundColor Green
Write-Host "Logs: `n  $OkLog`n  $ErrLog"
#endregion -----------------------------------------------------------
