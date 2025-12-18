$Server = 'vmdc2.tmgprivate.com'
$Username = 'codyparton'


Enter-PSSession -ComputerName $Server -Credential (Get-Credential)



Get-EventLog -LogName Security -InstanceId 4725 -After (Get-Date).AddDays(-1)
Get-EventLog -LogName Security -InstanceId 4740 -After (Get-Date).AddDays(-1)




Get-ADUser $Username -Properties userAccountControl | Select Name,Enabled,userAccountControl,whenChanged,whenChangedBy



Get-ADReplicationAttributeMetadata `
  -Object "CN=codyparton,OU=Users,DC=tmgprivate,DC=com" `
  -Server $Server `
| Where-Object { $_.AttributeName -eq 'userAccountControl' } `
| Select AttributeName, LastOriginatingChangeTime, LastOriginatingChangeDirectoryServerIdentity


Get-ADUser $Usernamer -Server $Server -Properties whenChanged | Select whenChanged

