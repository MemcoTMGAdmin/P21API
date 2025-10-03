$PicTick = "613163"
$TracNum  = "8989898989"
$PriceToEnter = "1587.39"
$CreateInvoice = "OFF"
$Body = @{
    Name = "Shipping"
    Transactions = @(
        @{
            Status = "New"
            DataElements = @(
                @{
                    Name = "TABPAGE_1.tp_1_dw_1"
                    BusinessObjectName = $null
                    Type = "Form"
                    Keys = @("pick_ticket_no")
                    Rows = @(
                        @{
                            Edits = @(
                                @{ Name = "pick_ticket_no"; Value = $PicTick; IgnoreIfEmpty = $false },
                                @{ Name = "tracking_no"; Value = $TracNum; IgnoreIfEmpty = $true },
                                @{ Name = "create_invoice"; Value = $CreateInvoice; IgnoreIfEmpty = $true }
                            )
                            RelativeDateEdits = @()
                        }
                    )
                },
                @{
                    Name = "TABPAGE_FREIGHT.tabpage_freight"
                    BusinessObjectName = $null
                    Type = "Form"
                    Keys = @()
                    Rows = @(
                        @{
                            Edits = @(
                                @{ Name = "freight_cd"; Value = ""; IgnoreIfEmpty = $true },
                                @{ Name = "freight_out"; Value = $PriceToEnter; IgnoreIfEmpty = $true }
                            )
                            RelativeDateEdits = @()
                        }
                    )
                }
            )
            Documents = $null
        }
    )
    Query = $null
    FieldMap = @()
    TransactionSplitMethod = 0
    Parameters = $null
}


# Convert body to JSON
$JsonBody = $Body | ConvertTo-Json -Depth 10 -Compress

# Set Bearer Token and API Endpoint
$Token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJwMjF1cy5lcGljb3JkaXN0cmlidXRpb24uY29tXFxqZWZmcGFydG9uIiwiYXVkIjoiIiwiUDIxLlNlc3Npb25JZCI6IjIwNDA2MDkwLWM0ZjItNGRjMy1iMjg3LWRiZDRhMmQ1ZGFiMiIsIm5iZiI6MTc1OTUxNzQ0NCwiZXhwIjoxNzU5NjAzODQ0LCJpYXQiOjE3NTk1MTc0NDQsImlzcyI6IlAyMS5Tb2EifQ.yuIFxluNEui0go2cPLW-Qelyp5WRJrO31d4O3Nk4EsU"
$Uri = "https://themiddletongroup-play-api.epicordistribution.com/uiserver0/api/v2/transaction"

# Define headers
$Headers = @{
    "Content-Type"  = "application/json"
    "Authorization" = "Bearer $Token"
}

# Send the request
$response = Invoke-RestMethod -Uri $Uri -Method Post -Headers $Headers -Body $JsonBody

# Output response
$response | ConvertTo-Json -Depth 5

Write-Host $Body
