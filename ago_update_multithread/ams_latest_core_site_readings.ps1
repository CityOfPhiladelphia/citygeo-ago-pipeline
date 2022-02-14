# Set our location to the called script's directory
# Important so logs and directories are where we expect them to be. Paths used in the script
# are relative so the script is a bit more portable.
Set-Location -Path $PSScriptRoot

# Republish to avoid record count checks since these guys will likely increase very fast
# We'll also be running this frequently I'm told, so don't log to the summary log files.
# Update: removing --republish flag for now as I've added functionality to check if the dataset in
# oracle databridge has changed.
$dataset = "LATEST_CORE_SITE_READINGS"
$args_string = "-d $dataset -o ago -p enterprise_perms -r"
Write-Host "Starting ago update for $dataset.."
$process = (Start-Process E:\arcpy\python.exe -ArgumentList "E:\Scripts\ago_update_multithread\ago_update.py $args_string" -PassThru -WindowStyle Hidden)

while ($process.HasExited -eq $false) {
    $duration =  New-TimeSpan -Start $process.StartTime
    if ($duration.Minutes -gt 50) {
                Stop-Process $process -Force
                Write-Host "Process for $dataset ran for over 50 minutes, force stopping it!"
                Exit 1
                Break
                }
    Start-Sleep 10
}

$duration =  New-TimeSpan -Start $process.StartTime
$code = $Process.ExitCode
$minutes = $duration.Minutes
$seconds = $duration.Seconds
if ($code -ne 0) {
    Write-Host "Process for $dataset failed with exit code $code!"
    Exit 1
}
elseif ($code -eq 0)
{
    Write-Host "Process for $dataset finished successfully in $minutes minutes and $seconds seconds."
    Exit 0
}