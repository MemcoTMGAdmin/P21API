$ApiBase        = 'https://themiddletongroup-api.epicordistribution.com'
$cutoffLocal = (Get-Date).AddMinutes(-60).ToString("yyyy-MM-ddTHH:mm:ss.fff")
$token   = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJwMjF1cy5lcGljb3JkaXN0cmlidXRpb24uY29tXFxjb2R5cGFydG9uIiwiYXVkIjoiIiwiUDIxLlNlc3Npb25JZCI6ImRjMmJiMGJlLTJhY2QtNDRlYy1hNmRlLTk2ZWFmODAxY2U3OSIsIm5iZiI6MTc3MDE0NTYzOSwiZXhwIjoxNzcwMjMyMDM5LCJpYXQiOjE3NzAxNDU2MzksImlzcyI6IlAyMS5Tb2EifQ.z98Mq6sgXIuSonZ6EtbEO1ExXW1DyOAzE-ZXT8999Lc'

$headers = @{
  Authorization = "Bearer $token"
  Accept        = "application/json"
}




$select = "pick_ticket_no,location_id,print_date,delete_flag,ship_date"
$filter = "print_date ge datetime'$cutoffLocal' and delete_flag eq 'N' and ship_date eq null"

$qs = "`$select=$([uri]::EscapeDataString($select))&`$filter=$([uri]::EscapeDataString($filter))&`$top=10"
$uri = "$ApiBase/data/erp/views/v1/p21_view_oe_pick_ticket?$qs"

$r = Invoke-RestMethod -Method Get -Uri $uri -Headers $headers

$r.value.Count
$r.value | Format-Table pick_ticket_no, location_id, print_date, delete_flag, ship_date -AutoSize