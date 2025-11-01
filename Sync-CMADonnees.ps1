<#
.SYNOPSIS
  Synchronises tables from the live CMADONNEES database into a local copy, with
  duplicate-safe updates using natural keys.

.NOTES
  - Reads optional overrides from environment variables set by the NestJS controller:
      SYNC_TABLES       Comma-separated list of table names to sync
      SYNC_KEYS_JSON    JSON mapping of table => [natural key columns]
  - Logs go to standard output for the frontend SSE stream; warnings/errors are surfaced too.
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$SourceDbPath,

    [Parameter(Mandatory = $true)]
    [string]$DestinationDbPath
)

$TablesToSync = @(
    @{ Name = 'Titres';      Key = 'id' },
    @{ Name = 'TypesTitres'; Key = 'id' },
    @{ Name = 'Detenteur';   Key = 'id' },
    @{ Name = 'coordonees';  Key = 'id' },
    @{ Name = 'TaxesSup';    Key = 'id' },
    @{ Name = 'DroitsEtabl'; Key = 'id' }
)

try {
    if ($env:SYNC_TABLES) {
        $names = ($env:SYNC_TABLES -split ',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }
        if ($names.Count -gt 0) {
            $override = @()
            foreach ($n in $names) { $override += @{ Name = $n; Key = 'id' } }
            $TablesToSync = $override
        }
    }
} catch {}

$ErrorActionPreference = 'Stop'

# .NET => OleDb type mapping
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
    if ($TypeHint -is [System.Data.OleDb.OleDbType]) { $oleType = $TypeHint }
    elseif ($TypeHint -is [System.Type]) { $oleType = $typeMap[$TypeHint.FullName] }
    if (-not $oleType) { $oleType = [System.Data.OleDb.OleDbType]::VarWChar }
    $param = $Command.Parameters.Add($Name, $oleType)
    $param.Value = [System.DBNull]::Value
    return $param
}

function Open-AccessConnection([string]$path) {
    if (-not (Test-Path $path)) { throw "Database not found: $path" }
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
    if (-not $schema) { throw "Unable to read column metadata for table [$tableName] in destination database." }
    $map = @{}
    foreach ($row in $schema.Rows) {
        $name = [string]$row['COLUMN_NAME']
        $upper = $name.ToUpperInvariant()
        $oleType = [System.Data.OleDb.OleDbType]($row['DATA_TYPE'])
        $dotNetType = Get-DotNetType $oleType
        $map[$upper] = [pscustomobject]@{ Name = $name; OleDbType = $oleType; DotNetType = $dotNetType }
    }
    return $map
}

function Get-RemoteColumns([System.Data.OleDb.OleDbConnection]$connection, [string]$tableName) {
    $schema = $connection.GetOleDbSchemaTable([System.Data.OleDb.OleDbSchemaGuid]::Columns, @($null, $null, $tableName, $null))
    if (-not $schema) { return @() }
    $columns = @()
    foreach ($row in ($schema.Rows | Sort-Object -Property ORDINAL_POSITION)) {
        $name = [string]$row['COLUMN_NAME']
        if ([string]::IsNullOrWhiteSpace($name)) { continue }
        $ordinal = 0
        if ($row['ORDINAL_POSITION'] -is [int]) { $ordinal = [int]$row['ORDINAL_POSITION'] }
        else { [int]::TryParse([string]$row['ORDINAL_POSITION'], [ref]$ordinal) | Out-Null }
        $columns += [pscustomobject]@{ Name = $name; OleDbType = [System.Data.OleDb.OleDbType]($row['DATA_TYPE']); Ordinal = $ordinal - 1 }
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
                if ($null -eq $value) { $value = [System.DBNull]::Value }
            } catch { $value = [System.DBNull]::Value; $columnOrder[$i].ReadErrors++ }
            $row[$i] = $value
        }
        $table.Rows.Add($row)
    }
    $table.EndLoadData()
    $reader.Close()
    return $table
}

function Convert-ForParameter {
    param($value, [Type]$targetType)
    if ($null -eq $value -or $value -is [System.DBNull]) { return [System.DBNull]::Value }
    try {
        if ($value -is [string]) {
            $value = $value.Replace([char]0xA0, ' ')
            $value = $value.Trim()
        }
        if ($targetType -eq [string]) { return [string]$value }
        if ($targetType -eq [bool]) {
            if ($value -is [string]) {
                $v = $value.Trim().ToLowerInvariant()
                if ($v -match '^(1|true|vrai|oui)$') { return $true }
                if ($v -match '^(0|false|faux|non)$') { return $false }
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
                if ([datetime]::TryParse($value, $culture, [System.Globalization.DateTimeStyles]::None, [ref]$parsed)) { return $parsed }
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
    } catch { return [System.DBNull]::Value }
}

function Get-RowValue {
    param([System.Data.DataRow]$row, $columnInfo)
    if ($null -eq $row -or $null -eq $columnInfo) { return $null }
    if ($columnInfo.PSObject.Properties.Match('RemoteOrdinal')) {
        $ord = $columnInfo.RemoteOrdinal
        if ($ord -is [int] -and $ord -ge 0 -and $ord -lt $row.Table.Columns.Count) { return $row[$ord] }
    }
    $remoteName = $columnInfo.RemoteName
    if (-not [string]::IsNullOrWhiteSpace([string]$remoteName) -and $row.Table.Columns.Contains($remoteName)) { return $row[$remoteName] }
    $localName = $columnInfo.LocalName
    if (-not [string]::IsNullOrWhiteSpace([string]$localName) -and $row.Table.Columns.Contains($localName)) { return $row[$localName] }
    return $null
}

function Escape-Identifier { param([string]$name) if ($null -eq $name) { return $null } return $name.Replace(']', ']]') }

# Natural-key defaults (used if auto-detect finds nothing)
$naturalKeyMap = @{
    'TypesTitres'     = @(@('Code'), @('Nom'))
    'TypesProcedures' = @(@('idTypeTitre','Procedure'))
}

function Get-UniqueCombos {
    param(
        [System.Data.OleDb.OleDbConnection]$connection,
        [string]$tableName,
        [string]$keyColumn
    )
    $results = @()
    try {
        $idx = $connection.GetOleDbSchemaTable([System.Data.OleDb.OleDbSchemaGuid]::Indexes, $null)
        if ($idx) {
            $byIndex = @{}
            foreach ($row in $idx.Rows) {
                try {
                    $tn = [string]$row['TABLE_NAME']
                    if ([string]::IsNullOrWhiteSpace($tn) -or -not $tn.Equals($tableName, [System.StringComparison]::InvariantCultureIgnoreCase)) { continue }
                    $ix = [string]$row['INDEX_NAME']
                    if ([string]::IsNullOrWhiteSpace($ix)) { continue }
                    $isUnique = $false
                    try { $isUnique = [bool]$row['UNIQUE'] } catch { try { $isUnique = ([string]$row['UNIQUE']).ToLowerInvariant() -eq 'true' -or [string]$row['UNIQUE'] -eq '1' } catch { $isUnique = $false } }
                    $isPk = $false
                    try { $isPk = [bool]$row['PRIMARY_KEY'] } catch { try { $isPk = [bool]$row['PRIMARY'] } catch { $isPk = $false } }
                    if (-not $isUnique -or $isPk) { continue }
                    $col = ''
                    try { $col = [string]$row['COLUMN_NAME'] } catch { $col = '' }
                    if ([string]::IsNullOrWhiteSpace($col)) { continue }
                    $ord = 0
                    try { $ord = [int]$row['ORDINAL_POSITION'] } catch { $ord = 0 }
                    if (-not $byIndex.ContainsKey($ix)) { $byIndex[$ix] = @() }
                    $byIndex[$ix] += [pscustomobject]@{ Name = $col; Ord = $ord }
                } catch {}
            }
            foreach ($kv in $byIndex.GetEnumerator()) {
                $cols = ($kv.Value | Sort-Object Ord | ForEach-Object { $_.Name })
                if ($cols.Count -eq 0) { continue }
                $filtered = @($cols | Where-Object { -not $_.Equals($keyColumn, [System.StringComparison]::InvariantCultureIgnoreCase) })
                if ($filtered.Count -gt 0) { $results += ,$filtered }
            }
        }
    } catch {}
    return $results
}

function Find-ExistingByNaturalKey {
    param(
        [System.Data.OleDb.OleDbConnection]$dest,
        [string]$tableName,
        [string]$keyColumn,
        [System.Data.DataRow]$row,
        [array]$commonColumnInfos,
        [array]$nkColumns,
        $tx
    )
    if (-not $nkColumns -or $nkColumns.Count -eq 0) { return $null }
    # Support either a single combo (array of strings) or multiple combos (array of arrays)
    $combos = @()
    if ($nkColumns[0] -is [System.Array]) { $combos = $nkColumns }
    else { $combos = @(@($nkColumns)) }

    foreach ($cols in $combos) {
        $availInfos = @()
        foreach ($col in $cols) {
            $ci = $commonColumnInfos | Where-Object { $_.LocalName.Equals($col, [System.StringComparison]::InvariantCultureIgnoreCase) } | Select-Object -First 1
            if ($ci) { $availInfos += $ci } else { $availInfos = @(); break }
        }
        if ($availInfos.Count -eq 0) { continue }
        $cmd = $dest.CreateCommand()
        $whereParts = @()
        for ($i=0; $i -lt $availInfos.Count; $i++) {
            $ci = $availInfos[$i]
            $isStr = $false; try { $isStr = ($ci.DotNetType -eq [string]) } catch { $isStr = $false }
            if ($isStr) {
                $whereParts += ("UCase(Replace(Trim([{0}]), Chr(160), ' ')) = ?" -f $ci.LocalName)
                $p = Add-Parameter $cmd ("@p_"+$ci.LocalName) $ci.OleDbType
                $v = Get-RowValue $row $ci; $sv = try { [string]$v } catch { '' }
                try { $sv = ($sv -replace "\u00A0", ' ').Trim().ToUpperInvariant() } catch { }
                $p.Value = $sv
            } else {
                $whereParts += ("[{0}] = ?" -f $ci.LocalName)
                $p = Add-Parameter $cmd ("@p_"+$ci.LocalName) $ci.OleDbType
                $v = Get-RowValue $row $ci
                $p.Value = Convert-ForParameter $v $ci.DotNetType
            }
        }
        $cmd.CommandText = "SELECT TOP 1 [$keyColumn] FROM [$tableName] WHERE " + ($whereParts -join ' AND ') + (" ORDER BY [{0}] DESC" -f $keyColumn)
        try { $cmd.Transaction = $tx } catch {}
        try {
            $found = $cmd.ExecuteScalar()
            if ($found -ne $null -and -not ($found -is [System.DBNull])) { return $found }
        } catch {}
    }
    return $null
}

function Find-ExistingByAnyColumns {
    param(
        [System.Data.OleDb.OleDbConnection]$dest,
        [string]$tableName,
        [string]$keyColumn,
        [System.Data.DataRow]$row,
        [array]$commonColumnInfos,
        [string[]]$columns,
        $tx
    )
    if (-not $columns -or $columns.Count -eq 0) { return $null }
    $pairs = @()
    foreach ($name in $columns) {
        $ci = $commonColumnInfos | Where-Object { $_.LocalName.Equals($name, [System.StringComparison]::InvariantCultureIgnoreCase) } | Select-Object -First 1
        if ($ci) {
            $val = Get-RowValue $row $ci
            try { if ($val -is [string]) { $val = ($val -replace "\u00A0", ' ').Trim() } } catch {}
            if ($null -ne $val -and -not ($val -is [System.DBNull]) -and ([string]$val).Length -gt 0) {
                $pairs += [pscustomobject]@{ Info = $ci; Value = $val }
            }
        }
    }
    if ($pairs.Count -eq 0) { return $null }
    $cmd = $dest.CreateCommand()
    $w = @()
    for ($i=0; $i -lt $pairs.Count; $i++) {
        $p = $pairs[$i]
        $isStr = $false; try { $isStr = ($p.Info.DotNetType -eq [string]) } catch { $isStr = $false }
        if ($isStr) {
            $w += ("UCase(Replace(Trim([{0}]), Chr(160), ' ')) = ?" -f $p.Info.LocalName)
            $par = Add-Parameter $cmd ("@p_"+$p.Info.LocalName) $p.Info.OleDbType
            $sv = try { [string]$p.Value } catch { '' }
            try { $sv = ($sv -replace "\u00A0", ' ').Trim().ToUpperInvariant() } catch {}
            $par.Value = $sv
        } else {
            $w += ("[{0}] = ?" -f $p.Info.LocalName)
            $par = Add-Parameter $cmd ("@p_"+$p.Info.LocalName) $p.Info.OleDbType
            $par.Value = Convert-ForParameter $p.Value $p.Info.DotNetType
        }
    }
    $cmd.CommandText = "SELECT TOP 1 [$keyColumn] FROM [$tableName] WHERE " + ($w -join ' OR ') + (" ORDER BY [{0}] DESC" -f $keyColumn)
    try { $cmd.Transaction = $tx } catch {}
    try { return $cmd.ExecuteScalar() } catch { return $null }
}

$scriptPath = try { $PSCommandPath } catch { try { $MyInvocation.MyCommand.Path } catch { '' } }
Write-Host ("[INFO] Démarrage de la synchronisation ({0})" -f $scriptPath)
Write-Host ("Source : {0}" -f $SourceDbPath)
Write-Host ("Destination : {0}" -f $DestinationDbPath)

$source = $null
$dest = $null

try {
    $source = Open-AccessConnection $SourceDbPath
    $dest   = Open-AccessConnection $DestinationDbPath

    foreach ($table in $TablesToSync) {
        $tableName = $table.Name
        $keyColumn = $table.Key

        Write-Host ("=== Syncing table [{0}] ===" -f $tableName)

        $localColumnMap = Get-LocalColumns $dest $tableName
        $remoteColumnInfos = Get-RemoteColumns $source $tableName
        $selectedRemoteColumns = @()
        foreach ($remoteInfo in $remoteColumnInfos) {
            $upper = $remoteInfo.Name.ToUpperInvariant()
            if ($localColumnMap.ContainsKey($upper) -and $allowedOleDbTypes -contains $remoteInfo.OleDbType) {
                $selectedRemoteColumns += [pscustomobject]@{
                    Name = $remoteInfo.Name
                    OleDbType = $remoteInfo.OleDbType
                    SelectOrdinal = $selectedRemoteColumns.Count
                    ReadErrors = 0
                }
            }
        }
        if ($selectedRemoteColumns.Count -eq 0) { Write-Warning '  No matching columns between source and destination - skipping.'; continue }

        $selectExpressions = @()
        for ($i = 0; $i -lt $selectedRemoteColumns.Count; $i++) { $selectExpressions += ("[{0}]" -f (Escape-Identifier $selectedRemoteColumns[$i].Name)) }
        $selectSql = "SELECT {0} FROM [{1}]" -f ($selectExpressions -join ', '), (Escape-Identifier $tableName)
        $remoteRows = Get-RemoteDataTable $source $selectSql $selectedRemoteColumns
        $colsListed = ($selectedRemoteColumns | ForEach-Object { $_.Name }) -join ', '
        Write-Host ("Selected columns: {0}" -f $colsListed)
        foreach ($colInfo in $selectedRemoteColumns) { if ($colInfo.ReadErrors -gt 0) { Write-Warning ("  Column [{0}] had {1} unread value(s)." -f $colInfo.Name, $colInfo.ReadErrors) } }
        if ($remoteRows.Rows.Count -eq 0) { Write-Host -ForegroundColor Yellow '  Source table is empty - nothing to do.'; continue }

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
        if ($commonColumnInfos.Count -eq 0) { Write-Warning '  No matching columns between source and destination - skipping.'; continue }

        $keyInfo = $commonColumnInfos | Where-Object { $_.LocalName.Equals($keyColumn, [System.StringComparison]::InvariantCultureIgnoreCase) } | Select-Object -First 1
        if (-not $keyInfo) { Write-Warning ("  Destination table [{0}] does not have key column [{1}] - skipping." -f $tableName, $keyColumn); continue }

        $updateColumnInfos = $commonColumnInfos | Where-Object { -not $_.LocalName.Equals($keyColumn, [System.StringComparison]::InvariantCultureIgnoreCase) }
        $updateColumns = $updateColumnInfos | ForEach-Object { $_.LocalName }
        $commonColumns = $commonColumnInfos | ForEach-Object { $_.LocalName }

        $existsCmd = $dest.CreateCommand()
        $existsCmd.CommandText = "SELECT COUNT(1) FROM [$tableName] WHERE [$keyColumn] = ?"
        $existsParam = Add-Parameter $existsCmd '@p_key' $keyInfo.OleDbType

        $updateCmd = $null
        if ($updateColumnInfos.Count -gt 0) {
            $setClause = ($updateColumns | ForEach-Object { "[$_]=?" }) -join ', '
            $updateCmd = $dest.CreateCommand()
            $updateCmd.CommandText = "UPDATE [$tableName] SET $setClause WHERE [$keyColumn] = ?"
            foreach ($info in $updateColumnInfos) { Add-Parameter $updateCmd ("@p_" + $info.LocalName) $info.OleDbType | Out-Null }
            Add-Parameter $updateCmd '@p_key' $keyInfo.OleDbType | Out-Null
        }

        $insertCmd = $dest.CreateCommand()
        $columnList = ($commonColumns | ForEach-Object { "[$_]" }) -join ', '
        $placeholders = ($commonColumns | ForEach-Object { '?' }) -join ', '
        $insertCmd.CommandText = "INSERT INTO [$tableName] ($columnList) VALUES ($placeholders)"
        $insertColumnInfos = $commonColumnInfos
        foreach ($info in $insertColumnInfos) { Add-Parameter $insertCmd ("@p_" + $info.LocalName) $info.OleDbType | Out-Null }

        # Optional no-key insert (when source key is NULL and dest key is AutoNumber)
        $insertCmdNoKey = $null
        $insertNoKeyColumnInfos = $null
        $insertNoKeyColumns = $commonColumns | Where-Object { -not $_.Equals($keyColumn, [System.StringComparison]::InvariantCultureIgnoreCase) }
        if ($insertNoKeyColumns.Count -gt 0) {
            $insertCmdNoKey = $dest.CreateCommand()
            $colList2 = ($insertNoKeyColumns | ForEach-Object { "[$_]" }) -join ', '
            $ph2 = ($insertNoKeyColumns | ForEach-Object { '?' }) -join ', '
            $insertCmdNoKey.CommandText = "INSERT INTO [$tableName] ($colList2) VALUES ($ph2)"
            $insertNoKeyColumnInfos = $commonColumnInfos | Where-Object { -not $_.LocalName.Equals($keyColumn, [System.StringComparison]::InvariantCultureIgnoreCase) }
            foreach ($info in $insertNoKeyColumnInfos) { Add-Parameter $insertCmdNoKey ("@p_" + $info.LocalName) $info.OleDbType | Out-Null }
        }

        $tx = $dest.BeginTransaction()
        $existsCmd.Transaction = $tx
        if ($updateCmd) { $updateCmd.Transaction = $tx }
        $insertCmd.Transaction = $tx
        if ($insertCmdNoKey) { $insertCmdNoKey.Transaction = $tx }

        $inserted = 0; $updated = 0
        $nkDetected = Get-UniqueCombos -connection $dest -tableName $tableName -keyColumn $keyColumn
        $nkConfigured = if ($nkDetected -and $nkDetected.Count -gt 0) { $nkDetected } else { $naturalKeyMap[$tableName] }
        try {
            if ($nkConfigured -and $nkConfigured.Count -gt 0) {
                $combosStr = ($nkConfigured | ForEach-Object { '(' + ((@($_)) -join ', ') + ')' }) -join ' OR '
                Write-Host ("  Natural keys: {0}" -f $combosStr)
            }
        } catch {}

        try {
            foreach ($row in $remoteRows.Rows) {
                $rawKey = Get-RowValue $row $keyInfo
                $keyValue = Convert-ForParameter $rawKey $keyInfo.DotNetType
                $forcedExisting = $false

                if ($keyValue -eq [System.DBNull]::Value) {
                    # Try to find destination row by natural key when key is NULL
                    $altId = Find-ExistingByNaturalKey -dest $dest -tableName $tableName -keyColumn $keyColumn -row $row -commonColumnInfos $commonColumnInfos -nkColumns $nkConfigured -tx $tx
                    if ($altId -ne $null -and -not ($altId -is [System.DBNull])) {
                        $keyValue = $altId
                        $forcedExisting = $true
                    }
                }

                if ($keyValue -ne [System.DBNull]::Value -and -not $forcedExisting) {
                    $existsParam.Value = $keyValue
                    $exists = [int]$existsCmd.ExecuteScalar()
                    if ($exists -eq 0 -and $nkConfigured) {
                        # Extra safety: if unique cols match an existing row, update that row instead of inserting
                        $altId2 = Find-ExistingByNaturalKey -dest $dest -tableName $tableName -keyColumn $keyColumn -row $row -commonColumnInfos $commonColumnInfos -nkColumns $nkConfigured -tx $tx
                        if ($altId2 -ne $null -and -not ($altId2 -is [System.DBNull])) {
                            $keyValue = $altId2
                            $forcedExisting = $true
                            $exists = 1
                        }
                    }
                    if ($exists -gt 0 -and $updateCmd) {
                        for ($i = 0; $i -lt $updateColumnInfos.Count; $i++) {
                            $info = $updateColumnInfos[$i]
                            $sourceValue = Get-RowValue $row $info
                            $updateCmd.Parameters[$i].Value = Convert-ForParameter $sourceValue $info.DotNetType
                        }
                        $updateCmd.Parameters[$updateColumnInfos.Count].Value = $keyValue
                        try { [void]$updateCmd.ExecuteNonQuery() } catch {
                            Write-Warning ("    Update failed for key {0} in table [{1}]" -f $rawKey, $tableName)
                            $_ | Format-List -Force | Out-String | Write-Warning
                            throw
                        }
                        $updated++
                        continue
                    }
                    elseif ($exists -eq 0) {
                        for ($i = 0; $i -lt $insertColumnInfos.Count; $i++) {
                            $info = $insertColumnInfos[$i]
                            $sourceValue = Get-RowValue $row $info
                            $insertCmd.Parameters[$i].Value = Convert-ForParameter $sourceValue $info.DotNetType
                        }
                        try { [void]$insertCmd.ExecuteNonQuery() } catch {
                            Write-Warning ("    Insert failed for key {0} in table [{1}]" -f $rawKey, $tableName)
                            $_ | Format-List -Force | Out-String | Write-Warning
                            # Attempt recovery by natural key -> update instead
                            $altId3 = Find-ExistingByNaturalKey -dest $dest -tableName $tableName -keyColumn $keyColumn -row $row -commonColumnInfos $commonColumnInfos -nkColumns $nkConfigured -tx $tx
                            if ($altId3 -ne $null -and -not ($altId3 -is [System.DBNull]) -and $updateCmd) {
                                for ($i2 = 0; $i2 -lt $updateColumnInfos.Count; $i2++) {
                                    $info2 = $updateColumnInfos[$i2]
                                    $src2 = Get-RowValue $row $info2
                                    $updateCmd.Parameters[$i2].Value = Convert-ForParameter $src2 $info2.DotNetType
                                }
                                $updateCmd.Parameters[$updateColumnInfos.Count].Value = $altId3
                                try { [void]$updateCmd.ExecuteNonQuery(); $updated++; continue } catch {}
                            } elseif ($insertCmdNoKey -ne $null) {
                                # Fallback: try insert without key (for AutoNumber destinations)
                                for ($j = 0; $j -lt $insertNoKeyColumnInfos.Count; $j++) {
                                    $infJ = $insertNoKeyColumnInfos[$j]
                                    $valJ = Get-RowValue $row $infJ
                                    $insertCmdNoKey.Parameters[$j].Value = Convert-ForParameter $valJ $infJ.DotNetType
                                }
                                try { [void]$insertCmdNoKey.ExecuteNonQuery(); $inserted++; continue } catch {
                                    Write-Warning ("    Insert(no-key,fallback) also failed in table [{0}]" -f $tableName)
                                }
                            }
                            throw
                        }
                        $inserted++
                        continue
                    }
                }

                # Key is NULL or we forced existing by natural key
                if ($forcedExisting -and $updateCmd) {
                    for ($i = 0; $i -lt $updateColumnInfos.Count; $i++) {
                        $info = $updateColumnInfos[$i]
                        $sourceValue = Get-RowValue $row $info
                        $updateCmd.Parameters[$i].Value = Convert-ForParameter $sourceValue $info.DotNetType
                    }
                    $updateCmd.Parameters[$updateColumnInfos.Count].Value = $keyValue
                    try { [void]$updateCmd.ExecuteNonQuery() } catch {
                        Write-Warning ("    Update(forced) failed in table [{0}]" -f $tableName)
                        $_ | Format-List -Force | Out-String | Write-Warning
                        throw
                    }
                    $updated++
                    continue
                }

                if ($insertCmdNoKey -ne $null) {
                    for ($i3 = 0; $i3 -lt $insertNoKeyColumnInfos.Count; $i3++) {
                        $info3 = $insertNoKeyColumnInfos[$i3]
                        $src3 = Get-RowValue $row $info3
                        $insertCmdNoKey.Parameters[$i3].Value = Convert-ForParameter $src3 $info3.DotNetType
                    }
                    try { [void]$insertCmdNoKey.ExecuteNonQuery() } catch {
                        Write-Warning ("    Insert(no-key) failed in table [{0}]" -f $tableName)
                        $_ | Format-List -Force | Out-String | Write-Warning
                        # One last attempt: update by natural key if possible
                        $altId4 = Find-ExistingByNaturalKey -dest $dest -tableName $tableName -keyColumn $keyColumn -row $row -commonColumnInfos $commonColumnInfos -nkColumns $nkConfigured -tx $tx
                        if ($altId4 -eq $null -or $altId4 -is [System.DBNull]) {
                            $altId4 = Find-ExistingByAnyColumns -dest $dest -tableName $tableName -keyColumn $keyColumn -row $row -commonColumnInfos $commonColumnInfos -columns @('Code','Nom','Libelle','Label','Name') -tx $tx
                        }
                        if ($altId4 -ne $null -and -not ($altId4 -is [System.DBNull]) -and $updateCmd) {
                            for ($j = 0; $j -lt $updateColumnInfos.Count; $j++) {
                                $infJ = $updateColumnInfos[$j]
                                $valJ = Get-RowValue $row $infJ
                                $updateCmd.Parameters[$j].Value = Convert-ForParameter $valJ $infJ.DotNetType
                            }
                            $updateCmd.Parameters[$updateColumnInfos.Count].Value = $altId4
                            try { [void]$updateCmd.ExecuteNonQuery(); $updated++; continue } catch {}
                        }
                        try {
                            $codeInfo = $commonColumnInfos | Where-Object { $_.LocalName.Equals('Code', [System.StringComparison]::InvariantCultureIgnoreCase) } | Select-Object -First 1
                            $nomInfo  = $commonColumnInfos | Where-Object { $_.LocalName.Equals('Nom',  [System.StringComparison]::InvariantCultureIgnoreCase) } | Select-Object -First 1
                            $codeVal = if ($codeInfo) { Get-RowValue $row $codeInfo } else { $null }
                            $nomVal  = if ($nomInfo)  { Get-RowValue $row $nomInfo  } else { $null }
                            $cv = try { [string]$codeVal } catch { '' }
                            $nv = try { [string]$nomVal } catch { '' }
                            Write-Warning ("    Debug NK values: Code='{0}', Nom='{1}'" -f $cv, $nv)
                        } catch {}
                        Write-Warning ("    Skipping row due to unique-constraint conflict not resolvable by detected or heuristic keys.")
                        continue
                    }
                    $inserted++
                }
            }

            $tx.Commit()
            Write-Host -ForegroundColor Green ('  Updated {0} row(s), inserted {1} row(s).' -f $updated, $inserted)
        } catch {
            $tx.Rollback()
            throw
        }
    }

    Write-Host ''
    Write-Host 'Sync complete.'
} finally {
    try { if ($source) { $source.Close(); $source.Dispose() } } catch {}
    try { if ($dest)   { $dest.Close();   $dest.Dispose()   } } catch {}
}
