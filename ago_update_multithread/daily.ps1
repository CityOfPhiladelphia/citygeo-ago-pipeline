# Set our location to the called script's directory
# Important so logs and directories are where we expect them to be. Paths used in the script
# are relative so the script is a bit more portable.
Set-Location -Path $PSScriptRoot

# Import Log-ToFile function
. .\custom_logger.ps1

$datasets =
    'BUILDING_CERTS',
    'CONTRACTOR_VIOLATIONS',
    'SUBCONTRACTORS',
    'REGISTERED_LOCAL_BUSINESSES',
    'COMPLAINTS'

$ignore_checks =
    'LaneClosure_EUN_XY',
    'LaneClosure_Master'

# Get start time so we can get a total time on script execution
$StartTime = $(get-date)

Set-Alias -Name 'python' -Value 'E:\arcpy\python.exe'

# Dictionary/hashtable to store process objects and to loop over them
$PIDList = @{}

# generic str array to track failures in our threads
$failed = New-Object Collections.Generic.List[String]

# Function that's called later to check if any processes have exited or timed out.
# If so, they are removed from the $PIDList var which is used to track active processes.
function Watch-Running-Processes () {
    foreach ($dataset in $PIDList.Keys) {
        # Get the process object which is the value
        $process = $PIDList.$dataset
        $duration =  New-TimeSpan -Start $process.StartTime
        $duration_minutes = $duration.Minutes
        $duration_seconds = $duration.Seconds
        $process_status = $process.HasExited

        # DEBUGGING!
        # Write out the entire hastable
        $debug_PIDList = $PIDList | Out-String
        Log-ToFile "Process for $dataset has a minute duration of $duration_minutes, and exit status is: $process_status" "duration_debug" $false
        Log-ToFile "Hastable looks like: $debug_PIDList" "duration_debug" $false
        # If a process has exited or it has taken over 2.5 hours, stop it.
        if ($process.HasExited) {
            # If the exit code is not 0, something failed that we may have not caught.
            $code = $Process.ExitCode
            if ($code -ne 0) {
                Log-ToFile "Process for $dataset failed with exit code $code!" "powershell"
                $failed.Add($dataset)
            }
            elseif ($code -eq 0)
            {
                Log-ToFile "Process for $dataset took $duration_minutes minutes and $duration_seconds seconds to complete." "powershell"
            }
            $PIDList.Remove($dataset)
            # Break enumeration for now because removing items from a list that is being actively iterated
            # is a no no and will generate an error. The while loop will start this function again anyway.
            Break
            }
        # NOTE: We could start it again with start-process, but it's probably too late if we waited 2.5 hours.
        # Update: let's set it for 50 minutes for the daily.
        if ($duration.Minutes -gt 50) {
                Stop-Process $process -Force
                $PIDList.Remove($dataset)
                Log-ToFile "Process for $dataset ran for over 50 minutes, force stopping it!" "powershell"
                $failed.Add($dataset)
                Break
                }
       }
}

# Main for loop that starts a python process per dataset and stores it in $PIDList
Foreach ($i in $datasets)
{
    # See if we should ignore checks for this dataset with the --republish/-r flag
    # For a dataset like 'LaneClosure_EUN_XY', the record count will vary wildly. So don't do row count
    # checks if it's one of those.
    if ($ignore_checks -Contains $i) {
        $args_string = "-d $i -o ago -p public_perms --republish"
    }
    else { $args_string = "-d $i -o ago -p public_perms" }
    Log-ToFile "Starting Process python E:\Scripts\ago_update_multithread\ago_update.py $args_string" "powershell"
    # note: aliases don't work with Start-Process
    $process = (Start-Process E:\arcpy\python.exe -ArgumentList "E:\Scripts\ago_update_multithread\ago_update.py $args_string" -PassThru -WindowStyle Hidden)
    # Key is dataset string, value is the process object
    $PIDList.Add($i,$process)

    # While the count of the list tracking process is underneath our target amount, while loop with a sleep
    # and check to see if any of the processes has finished or has been running for 2.5 hours.
    while ($PIDList.count -ge 9) {
        Start-Sleep 10
        Watch-Running-Processes
        }
}

# Once we've finished starting process for all datasets, wait for any remaining processes to finish.
while ($PIDList.count -ne 0) {
    Start-Sleep 10
    Watch-Running-Processes
}

$elapsedTime = $(get-date) - $StartTime
$totalTime = "{0:HH:mm:ss}" -f ([datetime]$elapsedTime.Ticks)
Log-ToFile "Total script execution time for $PSCommandPath is: $totalTime" "powershell"
if ( $failed.count -gt 0 ) {
    Write-Host "These datasets failed or timed out: '$failed'. Check the logs."
    Write-Host "Finished."
    Exit 1
}
Write-Host "Finished."
Exit 0
