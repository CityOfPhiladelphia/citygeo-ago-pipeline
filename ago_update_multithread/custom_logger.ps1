function Log-ToFile($msg, $log_name, $stdout=$true) {
    $the_date = Get-Date -UFormat "%Y-%m-%d %H:%M:%S"
    $msg_formatted = -join($the_date, " - POWERSHELL_INFO - ", $msg)
    $log_name_joined = -join($log_name, "-log.txt")
    # Don't put the powershell log in the logs directory if it's just "powershell"
    # because otherwise it's hard to see it.
    if ($log_name -eq 'powershell') {
         $log_path = Join-Path -Path $PSScriptRoot -ChildPath $log_name_joined
    }
    else {
        $log_path_dir = Join-Path -Path $PSScriptRoot -ChildPath 'logs'
        $log_path = Join-Path -Path $log_path_dir -ChildPath $log_name_joined
    }
    if (!(Test-Path $log_path))
        { New-Item -itemType File -Path $log_path }
    # Set permissions on the log file in case python made the log file and powershell can't access it.
    $rule=new-object System.Security.AccessControl.FileSystemAccessRule ("BUILTIN\Users","FullControl","Allow")
    $acl = Get-ACL $log_path
    $acl.SetAccessRule($rule)

    Add-Content -Path $log_path -Encoding Ascii -Value "$msg_formatted"
    # also write to stdout so it's logged in jenkins console output
    if ($stdout -eq $true) {
        Write-Host $msg_formatted
    }
}