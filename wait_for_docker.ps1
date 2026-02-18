
$maxRetries = 30
$retryCount = 0

Write-Host "Waiting for Docker Engine to be ready..."

while ($retryCount -lt $maxRetries) {
    try {
        docker info > $null 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Docker is ready!"
            docker-compose up -d
            exit 0
        }
    } catch {
        # Ignore errors
    }
    
    Write-Host -NoNewline "."
    Start-Sleep -Seconds 2
    $retryCount++
}

Write-Error "Docker failed to start within the timeout period."
exit 1
