$directory = "C:\Users\Vinicius Luiz\Desktop\Scripts\data_replication_python_service"
$outputFile = "C:\Users\Vinicius Luiz\Desktop\directory_structure.txt"

Get-ChildItem -Path $directory -Recurse -Force | 
Where-Object { 
    $_.FullName -notlike "*\venv\*" -and 
    $_.FullName -notlike "*\.git\*" 
} |
ForEach-Object {
    if ($_.PSIsContainer) {
        "[Pasta] $($_.FullName)"
    } else {
        "[Arquivo] $($_.FullName)"
    }
} | Out-File -FilePath $outputFile -Encoding UTF8

Write-Host "Estrutura de diretórios salva em $outputFile"

taskkill /F /IM python.exe