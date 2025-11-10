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
    [int]$Throttle = 1,  # Ignored in PS5 version (serial only)

    [Parameter(Mandatory = $false)]
    [ValidateSet('New','Update')]
    [string]$TransactionStatus = 'New'   # flip if your endpoint insists on "New"
)

$ProgressPreference = 'SilentlyContinue'

# --- Environment toggles ---
$EnvName           = 'DEV'  # 'DEV' | 'PROD' (informational)
$P21ApiBase        = 'https://themiddletongroup-play-api.epicordistribution.com'
$TransactionUri    = "$P21ApiBase/uiserver0/api/v2/transaction"
$DryRun            = $false
# --- SQL connection info (SQL auth ONLY) ---
$SqlServer         = 'p21us-read06.epicordistribution.com,50135'
$SqlDatabase       = 'az_130611_live'
$SqlUser           = 'readonly_130611_live'
$SqlPassword       = 'TWbrLcyz!5s69ab5p'  # TODO: move to SecretStore/DPAPI for prod

# --- T-SQL: MUST RETURN columns [item_id], [status]; extras are fine ---
$Tsql = @'
DECLARE @today_utc date = CAST(SYSUTCDATETIME() AS date);
DECLARE @d45  date = DATEADD(DAY, -45, @today_utc);
DECLARE @d90  date = DATEADD(DAY, -90, @today_utc);

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
SELECT
    im.item_id,
    q.qty,
    li.last_invoiced,
    im.class_id2,
    fr.first_received,
    CASE
        /* 1) Seasonal override */
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
# Accept: $null, any seasonal-like string ('seas' anywhere), or strict DEAD/UNDEAD/INACTIVE
$AllowedStatuses = 'DEAD','UNDEAD','INACTIVE'
#endregion -----------------------------------------------------------

#region -------------------- Token acquisition ----------------------
function Get-P21Token {
    param([string]$AuthBase = "https://themiddletongroup-play-api.epicordistribution.com")
    $AuthUri = "$AuthBase/api/security/token/v2"
    $ClientSecret = "c74ec0f8-220e-4203-a350-051a2bbe0bf4"  # <-- replace
    $GrantType = "client_credentials"

    $Headers = @{ "Accept"="application/json"; "Content-Type"="application/json" }
    $Body = @{ "ClientSecret"=$ClientSecret; "GrantType"=$GrantType } | ConvertTo-Json -Compress

    try {
        $response = Invoke-RestMethod -Uri $AuthUri -Method Post -Headers $Headers -Body $Body -TimeoutSec 60
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
    $cmd.CommandTimeout = 600

    try {
        $conn.Open()
        $reader = $cmd.ExecuteReader()
        $table.Load($reader)
        $reader.Close()
        return $table
    }
    finally {
        $conn.Close(); $conn.Dispose()
    }
}
#endregion -----------------------------------------------------------

#region -------------------- API helpers ----------------------------
function New-P21ItemStatusBody {
    param(
        [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$ItemId,
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
                            @{ Edits=@(@{ Name='item_id'; Value=$ItemId; IgnoreIfEmpty=$true }); RelativeDateEdits=@() }
                        )
                    },
                    @{
                        Name               = 'TABPAGE_CLASSES.classes'
                        BusinessObjectName = $null
                        Type               = 'Form'
                        Keys               = @()
                        Rows               = @(
                            @{ Edits=@(@{ Name='class_id2'; Value=$NewStatus; IgnoreIfEmpty=$false }); RelativeDateEdits=@() }
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

function Get-RowValue {
    param(
        [Parameter(Mandatory=$true)]$Row,
        [Parameter(Mandatory=$true)][string]$Name
    )
    # DataRow / DataRowView use indexer; PSObjects use dot
    if ($Row -is [System.Data.DataRow] -or $Row -is [System.Data.DataRowView]) {
        return $Row[$Name]
    } else {
        return $Row.$Name
    }
}

function Normalize-Rows {
    param([Parameter(Mandatory=$true)]$Data)

    if ($null -eq $Data) { throw "Query returned null." }

    if ($Data -is [System.Data.DataTable]) {
        return ,$Data.Rows   # force array with unary comma
    }

    if ($Data -is [System.Data.DataSet]) {
        if ($Data.Tables.Count -lt 1) { throw "DataSet has no tables." }
        return ,$Data.Tables[0].Rows
    }

    # Already an enumerable of rows/objects → wrap as array
    if ($Data -is [System.Collections.IEnumerable]) {
        return @($Data)
    }

    throw "Unexpected data type: $($Data.GetType().FullName)"
}


#region -------------------- Main flow --------------------------------
# --- Logging ---
$ts        = (Get-Date).ToString('yyyyMMdd_HHmmss')
$LogDir    = Join-Path -Path (Get-Location) -ChildPath 'logs'
$OkLog     = Join-Path $LogDir "p21_status_updates_ok_$ts.csv"
$ErrLog    = Join-Path $LogDir "p21_status_updates_err_$ts.csv"
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

if ($Throttle -gt 1) { Write-Warning "PS5 version runs serially. -Throttle is ignored." }
Write-Host "Environment: $EnvName  DryRun: $DryRun  TxStatus: $TransactionStatus" -ForegroundColor Cyan

# 1) Token
$Token = Get-P21Token

# 2) Data
$dataRaw = Invoke-SqlQuery -Server $SqlServer -Database $SqlDatabase -Query $Tsql -UserName $SqlUser -Password $SqlPassword
$rows    = Normalize-Rows $dataRaw
if ($rows.Count -eq 0) {
    Write-Host "No rows returned from SQL. Nothing to do." -ForegroundColor Yellow
    return
}

# 3) Validate shape & statuses (inspect the first row’s property/columns list)
$first = $rows | Select-Object -First 1
$cols  = if ($first -is [System.Data.DataRow] -or $first -is [System.Data.DataRowView]) {
            $first.Table.Columns | ForEach-Object ColumnName
         } else {
            $first.PSObject.Properties.Name
         }
foreach ($col in @('item_id','status')) {
    if ($cols -notcontains $col) { throw "SQL result is missing required column: $col" }
}

# 3a) empty item_id check
$badIdRows = @()
foreach ($r in $rows) {
    $iidObj = Get-RowValue -Row $r -Name 'item_id'
    $iid    = if ($null -ne $iidObj) { $iidObj.ToString().Trim() } else { '' }
    if ([string]::IsNullOrWhiteSpace($iid)) {
        $badIdRows += [pscustomobject]@{
            item_id = $iidObj
            status  = (Get-RowValue -Row $r -Name 'status')
            reason  = 'empty_item_id_after_trim'
        }
    }
}
if ($badIdRows.Count -gt 0) {
    $badPath = Join-Path $LogDir "rejected_empty_itemid_$ts.csv"
    $badIdRows | Export-Csv -NoTypeInformation -Path $badPath
    throw "Aborting: $($badIdRows.Count) row(s) with empty/whitespace item_id. See $badPath"
}

# 3b) status guard
$badStatusRows = @()
foreach ($r in $rows) {
    $sObj = Get-RowValue -Row $r -Name 'status'
    if ($null -eq $sObj) { continue }
    $s = $sObj.ToString()
    if ($s -match '(?i)seas') { continue }
    if (-not $AllowedStatuses.Contains($s.ToUpperInvariant())) {
        $badStatusRows += [pscustomobject]@{
            item_id = (Get-RowValue -Row $r -Name 'item_id')
            status  = $s
            reason  = 'status_not_allowed'
        }
    }
}
if ($badStatusRows.Count -gt 0) {
    $badPath = Join-Path $LogDir "rejected_status_$ts.csv"
    $badStatusRows | Export-Csv -NoTypeInformation -Path $badPath
    throw "Aborting: found $($badStatusRows.Count) row(s) with invalid status. See $badPath"
}

# 5) Process rows (serial)
$ok   = @()
$errb = @()

foreach ($r in $rows) {
    try {
        $itemIdObj = Get-RowValue -Row $r -Name 'item_id'
        $itemId    = if ($null -ne $itemIdObj) { $itemIdObj.ToString().Trim() } else { '' }
        $statusObj = Get-RowValue -Row $r -Name 'status'
        $newStatus = if ($null -ne $statusObj) { $statusObj.ToString().ToUpperInvariant() } else { $null }

        if ([string]::IsNullOrWhiteSpace($itemId)) { throw "empty item_id after ToString()+Trim()" }

        $jsonBody  = New-P21ItemStatusBody -ItemId $itemId -NewStatus $newStatus -TransactionStatus $TransactionStatus

        if ($DryRun) {
            $ok += [pscustomobject]@{ item_id=$itemId; requestedStatus=$newStatus; httpStatus='DRYRUN'; responseId=$null }
            continue
        }

        $resp = Invoke-RestMethod -Uri $TransactionUri -Method Post -Headers @{ 'Authorization'="Bearer $Token"; 'Content-Type'='application/json' } -Body $jsonBody -TimeoutSec 120 -MaximumRedirection 0
        $respId = $null; if ($resp -and $resp.PSObject.Properties.Name -contains 'id') { $respId = $resp.id }

        $ok += [pscustomobject]@{ item_id=$itemId; requestedStatus=$newStatus; httpStatus=200; responseId=$respId }
    } catch {
        $errb += [pscustomobject]@{
            item_id         = (Get-RowValue -Row $r -Name 'item_id')
            requestedStatus = (Get-RowValue -Row $r -Name 'status')
            error           = $_.Exception.Message
        }
    }
    Write-Host "$itemId Complete"
}

# 6) Logs
$ts2     = (Get-Date).ToString('yyyyMMdd_HHmmss')
$OkLog2  = Join-Path $LogDir "p21_status_updates_ok_$ts2.csv"
$ErrLog2 = Join-Path $LogDir "p21_status_updates_err_$ts2.csv"
$ok   | Export-Csv -NoTypeInformation -Path $OkLog2
$errb | Export-Csv -NoTypeInformation -Path $ErrLog2

Write-Host ("Done. OK: {0}  ERR: {1}" -f $ok.Count, $errb.Count) -ForegroundColor Green
Write-Host "Logs:`n  $OkLog2`n  $ErrLog2"
#endregion -----------------------------------------------------------