#requires -Version 5
<#
.SYNOPSIS
    Synchronises tables from the “live” CMADONNEES database into the local customised copy.

.DESCRIPTION
    For each table listed in $TablesToSync the script copies all rows from the source
    database into the destination database.  If a row already exists (matched on the key
    column) it updates only the columns that exist in the source table.  Rows that are not
    present locally are inserted.  Extra columns that exist only in the local database are
    never modified and tables that exist only locally are left untouched.

    Requirements:
      • Microsoft Access Database Engine (ACE OLEDB) installed (32‑ or 64‑bit to match PowerShell).
      • Source database accessible (UNC share or mapped drive).
      • No-one should have the destination MDB open when the script runs.
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$SourceDbPath ,      # e.g. '\\SYNC-SERVER\data\cmadonnees.mdb'

    [Parameter(Mandatory = $true)]
    [string]$DestinationDbPath # e.g. 'C:\Data\cmadonneess.mdb'
)

$TablesToSync = @(
   @{ Name = 'Titres';        Key = 'id' },
    @{ Name = 'TypesTitres';   Key = 'id' },
    @{ Name = 'Detenteur';     Key = 'id' },
    @{ Name = 'coordonees';    Key = 'id' },
    @{ Name = 'TaxesSup';      Key = 'id' },
    @{ Name = 'DroitsEtabl';   Key = 'id' }
    # Add/remove tables as needed; do NOT include your local-only tables (e.g. templates).
)

$ErrorActionPreference = 'Stop'
# Mapping .NET types to OleDb types
$typeMap = @{
    'System.String'   = [System.Data.OleDb.OleDbType]::VarWChar
    'System.Int32'    = [System.Data.OleDb.OleDbType]::Integer
    'System.Int16'    = [System.Data.OleDb.OleDbType]::SmallInt
    'System.Int64'    = [System.Data.OleDb.OleDbType]::BigInt
    'System.Decimal'  = [System.Data.OleDb.OleDbType]::Decimal
    'System.Double'   = [System.Data.OleDb.OleDbType]::Double
    'System.Single'   = [System.Data.OleDb.OleDbType]::Single
    'System.Boolean'  = [System.Data.OleDb.OleDbType]::Boolean
    'System.DateTime' = [System.Data.OleDb.OleDbType]::Date
    'System.Byte[]'   = [System.Data.OleDb.OleDbType]::Binary
}

$allowedOleDbTypes = @(
    [System.Data.OleDb.OleDbType]::VarWChar,
    [System.Data.OleDb.OleDbType]::LongVarWChar,
    [System.Data.OleDb.OleDbType]::VarChar,
    [System.Data.OleDb.OleDbType]::LongVarChar,
    [System.Data.OleDb.OleDbType]::WChar,
    [System.Data.OleDb.OleDbType]::Char,
    [System.Data.OleDb.OleDbType]::Numeric,
    [System.Data.OleDb.OleDbType]::Decimal,
    [System.Data.OleDb.OleDbType]::Currency,
    [System.Data.OleDb.OleDbType]::Integer,
    [System.Data.OleDb.OleDbType]::SmallInt,
    [System.Data.OleDb.OleDbType]::BigInt,
    [System.Data.OleDb.OleDbType]::TinyInt,
    [System.Data.OleDb.OleDbType]::Double,
    [System.Data.OleDb.OleDbType]::Single,
    [System.Data.OleDb.OleDbType]::Boolean,
    [System.Data.OleDb.OleDbType]::Date,
    [System.Data.OleDb.OleDbType]::DBDate,
    [System.Data.OleDb.OleDbType]::DBTime,
    [System.Data.OleDb.OleDbType]::DBTimeStamp,
    [System.Data.OleDb.OleDbType]::Guid
)

function Add-Parameter {
    param(
        [System.Data.OleDb.OleDbCommand]$Command,
        [string]$Name,
        $TypeHint
    )
    $oleType = $null
    if ($TypeHint -is [System.Data.OleDb.OleDbType]) {
        $oleType = $TypeHint
    } elseif ($TypeHint -is [System.Type]) {
        $oleType = $typeMap[$TypeHint.FullName]
    }
    if (-not $oleType) {
        $oleType = [System.Data.OleDb.OleDbType]::VarWChar
    }
    $param = $Command.Parameters.Add($Name, $oleType)
    $param.Value = [System.DBNull]::Value
    return $param
}


function Open-AccessConnection([string]$path) {
    if (-not (Test-Path $path)) {
        throw "Database not found: $path"
    }
    $connString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=$path;Persist Security Info=False;"
    $conn = New-Object System.Data.OleDb.OleDbConnection($connString)
    $conn.Open()
    return $conn
}

function Get-DotNetType([System.Data.OleDb.OleDbType]$oleType) {
    switch ($oleType) {
        ([System.Data.OleDb.OleDbType]::BigInt)      { return [System.Int64] }
        ([System.Data.OleDb.OleDbType]::Integer)     { return [System.Int32] }
        ([System.Data.OleDb.OleDbType]::SmallInt)    { return [System.Int16] }
        ([System.Data.OleDb.OleDbType]::TinyInt)     { return [System.Byte] }
        ([System.Data.OleDb.OleDbType]::Decimal)     { return [System.Decimal] }
        ([System.Data.OleDb.OleDbType]::Currency)    { return [System.Decimal] }
        ([System.Data.OleDb.OleDbType]::Numeric)     { return [System.Decimal] }
        ([System.Data.OleDb.OleDbType]::Double)      { return [System.Double] }
        ([System.Data.OleDb.OleDbType]::Single)      { return [System.Single] }
        ([System.Data.OleDb.OleDbType]::Date)        { return [System.DateTime] }
        ([System.Data.OleDb.OleDbType]::DBDate)      { return [System.DateTime] }
        ([System.Data.OleDb.OleDbType]::DBTime)      { return [System.DateTime] }
        ([System.Data.OleDb.OleDbType]::DBTimeStamp) { return [System.DateTime] }
        ([System.Data.OleDb.OleDbType]::Boolean)     { return [System.Boolean] }
        default                                      { return [System.String] }
    }
}

function Get-LocalColumns([System.Data.OleDb.OleDbConnection]$connection, [string]$tableName) {
    $schema = $connection.GetOleDbSchemaTable([System.Data.OleDb.OleDbSchemaGuid]::Columns, @($null, $null, $tableName, $null))
    if (-not $schema) {
        throw "Unable to read column metadata for table [$tableName] in destination database."
    }
    $map = @{}
    foreach ($row in $schema.Rows) {
        $name = [string]$row["COLUMN_NAME"]
        $upper = $name.ToUpperInvariant()
        $oleType = [System.Data.OleDb.OleDbType]($row["DATA_TYPE"])
        $dotNetType = Get-DotNetType $oleType
        $map[$upper] = [pscustomobject]@{
            Name        = $name
            OleDbType   = $oleType
            DotNetType  = $dotNetType
        }
    }
    return $map
}

function Get-RemoteColumns([System.Data.OleDb.OleDbConnection]$connection, [string]$tableName) {
    $schema = $connection.GetOleDbSchemaTable([System.Data.OleDb.OleDbSchemaGuid]::Columns, @($null, $null, $tableName, $null))
    if (-not $schema) {
        return @()
    }
    $columns = @()
    foreach ($row in ($schema.Rows | Sort-Object -Property ORDINAL_POSITION)) {
        $name = [string]$row["COLUMN_NAME"]
        if ([string]::IsNullOrWhiteSpace($name)) { continue }
        $ordinal = 0
        if ($row["ORDINAL_POSITION"] -is [int]) {
            $ordinal = [int]$row["ORDINAL_POSITION"]
        } else {
            [int]::TryParse([string]$row["ORDINAL_POSITION"], [ref]$ordinal) | Out-Null
        }
        $columns += [pscustomobject]@{
            Name           = $name
            OleDbType      = [System.Data.OleDb.OleDbType]($row["DATA_TYPE"])
            Ordinal        = $ordinal - 1
        }
    }
    return $columns
}

function Get-RemoteDataTable {
    param(
        [System.Data.OleDb.OleDbConnection]$connection,
        [string]$sql,
        [array]$columnOrder
    )

    $command = $connection.CreateCommand()
    $command.CommandText = $sql

    $reader = $command.ExecuteReader()
    $table = New-Object System.Data.DataTable

    foreach ($colInfo in $columnOrder) {
        $column = New-Object System.Data.DataColumn($colInfo.Name, [object])
        [void]$table.Columns.Add($column)
    }

    $table.BeginLoadData()
    while ($reader.Read()) {
        $row = $table.NewRow()
        for ($i = 0; $i -lt $columnOrder.Count; $i++) {
            $value = $null
            try {
                $value = $reader.GetValue($i)
                if ($null -eq $value) {
                    $value = [System.DBNull]::Value
                }
            } catch {
                $value = [System.DBNull]::Value
                $columnOrder[$i].ReadErrors++
            }
            $row[$i] = $value
        }
        $table.Rows.Add($row)
    }
    $table.EndLoadData()
    $reader.Close()
    return $table
}

function Convert-ForParameter {
    param(
        $value,
        [Type]$targetType
    )
    if ($null -eq $value -or $value -is [System.DBNull]) {
        return [System.DBNull]::Value
    }
    try {
        if ($value -is [string]) {
            $value = $value.Replace([char]0xA0, ' ')
            $value = $value.Trim()
        }
        if ($targetType -eq [string]) {
            return [string]$value
        }
        if ($targetType -eq [bool]) {
            if ($value -is [string]) {
                $v = $value.Trim().ToLowerInvariant()
                if ($v -match '^(1|true|vrai|oui|oui\\s*)$') { return $true }
                if ($v -match '^(0|false|faux|non|non\\s*)$') { return $false }
            }
            return [bool]$value
        }
        if ($targetType -eq [datetime]) {
            if ($value -is [datetime]) { return $value }
            $cultures = @(
                [System.Globalization.CultureInfo]::InvariantCulture,
                [System.Globalization.CultureInfo]::GetCultureInfo('fr-FR'),
                [System.Globalization.CultureInfo]::GetCultureInfo('en-US')
            )
            foreach ($culture in $cultures) {
                $parsed = $null
                if ([datetime]::TryParse($value, $culture, [System.Globalization.DateTimeStyles]::None, [ref]$parsed)) {
                    return $parsed
                }
            }
            return [System.DBNull]::Value
        }
        if ($targetType -eq [decimal] -or $targetType -eq [double] -or $targetType -eq [float]) {
            if ($value -is [string]) {
                $clean = ($value -replace '[^0-9,.-]', '').Replace(',', '.')
                if ([string]::IsNullOrWhiteSpace($clean)) { return [System.DBNull]::Value }
                return [System.Convert]::ChangeType($clean, $targetType, [System.Globalization.CultureInfo]::InvariantCulture)
            }
            return [System.Convert]::ChangeType($value, $targetType, [System.Globalization.CultureInfo]::InvariantCulture)
        }
        if ($targetType -eq [int] -or $targetType -eq [long] -or $targetType -eq [short] -or $targetType -eq [byte]) {
            if ($value -is [string]) {
                $clean = ($value -replace '[^0-9-]', '')
                if ([string]::IsNullOrWhiteSpace($clean)) { return [System.DBNull]::Value }
                return [System.Convert]::ChangeType($clean, $targetType, [System.Globalization.CultureInfo]::InvariantCulture)
            }
            return [System.Convert]::ChangeType($value, $targetType, [System.Globalization.CultureInfo]::InvariantCulture)
        }
        return [System.Convert]::ChangeType($value, $targetType, [System.Globalization.CultureInfo]::InvariantCulture)
    } catch {
        Write-Verbose "Conversion failed for value '$value' to type $($targetType.FullName); using DBNull."
        return [System.DBNull]::Value
    }
}

function Get-RowValue {
    param(
        [System.Data.DataRow]$row,
        $columnInfo
    )
    if ($null -eq $row -or $null -eq $columnInfo) {
        return $null
    }

    if ($columnInfo.PSObject.Properties.Match('RemoteOrdinal')) {
        $ordinal = $columnInfo.RemoteOrdinal
        if ($ordinal -is [int] -and $ordinal -ge 0 -and $ordinal -lt $row.Table.Columns.Count) {
            return $row[$ordinal]
        }
    }

    $remoteName = $columnInfo.RemoteName
    if (-not [string]::IsNullOrWhiteSpace([string]$remoteName) -and $row.Table.Columns.Contains($remoteName)) {
        return $row[$remoteName]
    }

    $localName = $columnInfo.LocalName
    if (-not [string]::IsNullOrWhiteSpace([string]$localName) -and $row.Table.Columns.Contains($localName)) {
        return $row[$localName]
    }

    return $null
}

function Escape-Identifier {
    param([string]$name)
    if ($null -eq $name) { return $null }
    return $name.Replace(']', ']]')
}

Write-Host "Source      : $SourceDbPath"
Write-Host "Destination : $DestinationDbPath"
Write-Host ""

$source  = $null
$dest    = $null

try {
    $source = Open-AccessConnection $SourceDbPath
    $dest   = Open-AccessConnection $DestinationDbPath

    foreach ($table in $TablesToSync) {
        $tableName = $table.Name
        $keyColumn = $table.Key

        Write-Host "=== Syncing table [$tableName] ==="

        $localColumnMap = Get-LocalColumns $dest $tableName

        $remoteColumnInfos = Get-RemoteColumns $source $tableName
        $selectedRemoteColumns = @()
        foreach ($remoteInfo in $remoteColumnInfos) {
            $upper = $remoteInfo.Name.ToUpperInvariant()
            if ($localColumnMap.ContainsKey($upper) -and $allowedOleDbTypes -contains $remoteInfo.OleDbType) {
                $selectedRemoteColumns += $remoteInfo
            }
        }

        if ($selectedRemoteColumns.Count -eq 0) {
            Write-Warning '  No matching columns between source and destination - skipping.'
            continue
        }

        $selectExpressions = @()
        for ($i = 0; $i -lt $selectedRemoteColumns.Count; $i++) {
            $remoteInfo = $selectedRemoteColumns[$i]
            $escaped = Escape-Identifier $remoteInfo.Name
            $selectExpressions += "[$escaped]"
            $selectedRemoteColumns[$i] = [pscustomobject]@{
                Name = $remoteInfo.Name
                OleDbType = $remoteInfo.OleDbType
                SelectOrdinal = $i
                ReadErrors = 0
            }
        }

        $escapedTableName = Escape-Identifier $tableName
        $selectSql = "SELECT {0} FROM [{1}]" -f ($selectExpressions -join ', '), $escapedTableName
        $remoteRows = Get-RemoteDataTable $source $selectSql $selectedRemoteColumns
        foreach ($colInfo in $selectedRemoteColumns) {
            if ($colInfo.ReadErrors -gt 0) {
                Write-Warning ("  Column [{0}] had {1} value(s) that could not be read and were replaced with NULL." -f $colInfo.Name, $colInfo.ReadErrors)
            }
        }

        if ($remoteRows.Rows.Count -eq 0) {
            Write-Host -ForegroundColor Yellow '  Source table is empty - nothing to do.'
            continue
        }

        if (-not $remoteRows.Columns.Contains($keyColumn)) {
            Write-Warning '  Key column [$keyColumn] not found in source table - skipping.'
            continue
        }

        $commonColumnInfos = @()
        foreach ($remoteInfo in $selectedRemoteColumns) {
            $upper = $remoteInfo.Name.ToUpperInvariant()
            if ($localColumnMap.ContainsKey($upper)) {
                $localInfo = $localColumnMap[$upper]
                $commonColumnInfos += [pscustomobject]@{
                    LocalName = $localInfo.Name
                    RemoteName = $remoteInfo.Name
                    RemoteOrdinal = $remoteInfo.SelectOrdinal
                    OleDbType = $localInfo.OleDbType
                    DotNetType = $localInfo.DotNetType
                }
            }
        }

        if ($commonColumnInfos.Count -eq 0) {
            Write-Warning '  No matching columns between source and destination - skipping.'
            continue
        }

        $keyInfo = $commonColumnInfos | Where-Object { $_.LocalName.Equals($keyColumn, [System.StringComparison]::InvariantCultureIgnoreCase) } | Select-Object -First 1
        if (-not $keyInfo) {
            Write-Warning '  Destination table [$tableName] does not have key column [$keyColumn] - skipping.'
            continue
        }

        $updateColumnInfos = $commonColumnInfos | Where-Object { -not $_.LocalName.Equals($keyColumn, [System.StringComparison]::InvariantCultureIgnoreCase) }
        $updateColumns = $updateColumnInfos | ForEach-Object { $_.LocalName }
        $commonColumns = $commonColumnInfos | ForEach-Object { $_.LocalName }

        $existsCmd = $dest.CreateCommand()
        $existsCmd.CommandText = "SELECT COUNT(1) FROM [$tableName] WHERE [$keyColumn] = ?"
        $existsParam = Add-Parameter $existsCmd "@p_key" $keyInfo.OleDbType

        $updateCmd = $null
        if ($updateColumnInfos.Count -gt 0) {
            $setClause = ($updateColumns | ForEach-Object { "[$_]=?" }) -join ', '
            $updateCmd = $dest.CreateCommand()
            $updateCmd.CommandText = "UPDATE [$tableName] SET $setClause WHERE [$keyColumn] = ?"
            foreach ($info in $updateColumnInfos) {
                Add-Parameter $updateCmd ("@p_" + $info.LocalName) $info.OleDbType | Out-Null
            }
            Add-Parameter $updateCmd "@p_key" $keyInfo.OleDbType | Out-Null
        }

        $insertCmd = $dest.CreateCommand()
        $columnList = ($commonColumns | ForEach-Object { "[$_]" }) -join ', '
        $placeholders = ($commonColumns | ForEach-Object { "?" }) -join ', '
        $insertCmd.CommandText = "INSERT INTO [$tableName] ($columnList) VALUES ($placeholders)"
        $insertColumnInfos = $commonColumnInfos
        foreach ($info in $insertColumnInfos) {
            Add-Parameter $insertCmd ("@p_" + $info.LocalName) $info.OleDbType | Out-Null
        }

        $tx = $dest.BeginTransaction()
        $existsCmd.Transaction = $tx
        if ($updateCmd) { $updateCmd.Transaction = $tx }
        $insertCmd.Transaction = $tx

        $inserted = 0
        $updated  = 0

        try {
            foreach ($row in $remoteRows.Rows) {
                $rawKey = Get-RowValue $row $keyInfo
                if ($null -eq $rawKey -or $rawKey -is [System.DBNull]) {
                    Write-Warning '  Skipping row with null/invalid key value in table [$tableName].'
                    continue
                }
                $keyValue = Convert-ForParameter $rawKey $keyInfo.DotNetType
                if ($keyValue -eq [System.DBNull]::Value) {
                    Write-Warning '  Skipping row with null/invalid key value in table [$tableName].'
                    continue
                }
                $existsParam.Value = $keyValue
                $exists = [int]$existsCmd.ExecuteScalar()

                if ($exists -gt 0 -and $updateCmd) {
                    for ($i = 0; $i -lt $updateColumnInfos.Count; $i++) {
                        $info = $updateColumnInfos[$i]
                        $sourceValue = Get-RowValue $row $info
                        $updateCmd.Parameters[$i].Value = Convert-ForParameter $sourceValue $info.DotNetType
                    }
                    $updateCmd.Parameters[$updateColumnInfos.Count].Value = $keyValue
                    try {
                        [void]$updateCmd.ExecuteNonQuery()
                    } catch {
                        Write-Warning ('    Update failed for key {0} in table [{1}]' -f $rawKey, $tableName)
                        Write-Warning ('    Columns: {0}' -f (($updateColumnInfos | ForEach-Object { $_.LocalName }) -join ', '))
                        $_ | Format-List -Force | Out-String | Write-Warning
                        throw
                    }
                    $updated++
                }
                elseif ($exists -eq 0) {
                    for ($i = 0; $i -lt $insertColumnInfos.Count; $i++) {
                        $info = $insertColumnInfos[$i]
                        $sourceValue = Get-RowValue $row $info
                        $insertCmd.Parameters[$i].Value = Convert-ForParameter $sourceValue $info.DotNetType
                    }
                    try {
                        [void]$insertCmd.ExecuteNonQuery()
                    } catch {
                        Write-Warning ('    Insert failed for key {0} in table [{1}]' -f $rawKey, $tableName)
                        Write-Warning ('    Columns: {0}' -f (($insertColumnInfos | ForEach-Object { $_.LocalName }) -join ', '))
                        $_ | Format-List -Force | Out-String | Write-Warning
                        throw
                    }
                    $inserted++
                }
            }

            $tx.Commit()
            Write-Host -ForegroundColor Green ('  Updated {0} row(s), inserted {1} row(s).' -f $updated, $inserted)
        }
        catch {
            $tx.Rollback()
            throw
        }
    }

    Write-Host ''
    Write-Host 'Sync complete.'
}
finally {
    if ($source)   { $source.Close();   $source.Dispose() }
    if ($dest)     { $dest.Close();     $dest.Dispose()   }
}

