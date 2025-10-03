$Body = @{
    Name = "Item"
    UseCodeValues = $false
    IgnoreDisabled = $false
    Transactions = @(
        @{
            Status = "New"
            DataElements = @(
                @{
                    Name = "TABPAGE_1.tp_1_dw_1"
                    BusinessObjectName = $null
                    Type = "Form"
                    Keys = @("item_id")
                    Rows = @(
                        @{
                            Edits = @(
                                @{
                                    Name = "item_id"
                                    Value = "1008111"
                                    IgnoreIfEmpty = $true
                                }
                            )
                            RelativeDateEdits = @()
                        }
                    )
                },
                @{
                    Name = "TABPAGE_CLASSES.classes"
                    BusinessObjectName = $null
                    Type = "Form"
                    Keys = @()
                    Rows = @(
                        @{
                            Edits = @(
                                @{
                                    Name = "class_id2"
                                    Value = $null
                                    IgnoreIfEmpty = $false
                                }
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

# Set Bearer Token and API EndpointeyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJwMjF1cy5lcGljb3JkaXN0cmlidXRpb24uY29tXFxqZWZmcGFydG9uIiwiYXVkIjoiIiwiUDIxLlNlc3Npb25JZCI6IjQ0NzVhODAyLTQxOTgtNGY3Zi04OWNkLWNhYTYwYzkzZTlhNCIsIm5iZiI6MTc1OTQyODM3MCwiZXhwIjoxNzU5NTE0NzcwLCJpYXQiOjE3NTk0MjgzNzAsImlzcyI6IlAyMS5Tb2EifQ.O7L57ORjjItj8iNnzcZEf55w0cQyZQKj5kBGyYLdwNc"
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
$response