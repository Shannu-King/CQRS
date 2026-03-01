$ErrorActionPreference = 'Stop'

$results = New-Object System.Collections.Generic.List[object]

function Add-Result {
    param(
        [int]$Req,
        [string]$Name,
        [bool]$Pass,
        [string]$Detail
    )

    $results.Add([pscustomobject]@{
        Requirement = $Req
        Name        = $Name
        Status      = if ($Pass) { 'PASS' } else { 'FAIL' }
        Detail      = $Detail
    })
}

function Assert-ContainsAll {
    param([string]$Text, [string[]]$Needles)

    foreach ($needle in $Needles) {
        if ($Text -notmatch [regex]::Escape($needle)) {
            return $false
        }
    }

    return $true
}

Write-Host 'Running full verification (mandatory + negative)...' -ForegroundColor Cyan

# 1) Compose + health
$composePs = docker compose ps
$servicesHealthy = @('ps3-db-1', 'ps3-broker-1', 'ps3-command-service-1', 'ps3-query-service-1', 'ps3-consumer-service-1')
$req1Pass = Assert-ContainsAll -Text $composePs -Needles $servicesHealthy
$req1Pass = $req1Pass -and ($composePs -match 'healthy')
Add-Result 1 'Compose services healthy' $req1Pass 'All required services running with healthy status'

# 2) .env.example exists + required vars
$envPath = '.env.example'
$envPass = Test-Path $envPath
$envText = if ($envPass) { Get-Content $envPath -Raw } else { '' }
$envNeedles = @('DATABASE_URL', 'READ_DATABASE_URL', 'BROKER_URL', 'COMMAND_SERVICE_PORT', 'QUERY_SERVICE_PORT')
$envPass = $envPass -and (Assert-ContainsAll -Text $envText -Needles $envNeedles)
Add-Result 2 '.env.example required vars' $envPass 'Contains DB, broker, and service port vars'

# 3) submission.json urls
$subPass = Test-Path 'submission.json'
if ($subPass) {
    $sub = Get-Content 'submission.json' -Raw | ConvertFrom-Json
    $subPass = ($sub.commandServiceUrl -eq 'http://localhost:8080' -and $sub.queryServiceUrl -eq 'http://localhost:8081')
}
Add-Result 3 'submission.json URLs' $subPass 'commandServiceUrl/queryServiceUrl are correct'

# 4) Write tables
$writeTableCount = [int](docker exec ps3-db-1 psql -U user -d write_db -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('products','orders','order_items');")
Add-Result 4 'Write model tables' ($writeTableCount -eq 3) "Found $writeTableCount/3 required tables"

# 5) Outbox required columns
$outboxCols = [int](docker exec ps3-db-1 psql -U user -d write_db -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='outbox' AND column_name IN ('id','topic','payload','published_at');")
Add-Result 5 'Outbox required columns' ($outboxCols -eq 4) "Found $outboxCols/4 required columns"

# Reset test data area (outbox only for timing checks)
docker exec ps3-db-1 psql -U user -d write_db -c "TRUNCATE TABLE outbox RESTART IDENTITY;" | Out-Null

# 6) POST /api/products
$productRes = Invoke-WebRequest -Method Post -Uri 'http://localhost:8080/api/products' -ContentType 'application/json' -Body '{"name":"Verify Product","category":"verify-cat","price":100,"stock":30}' -UseBasicParsing
$productObj = $productRes.Content | ConvertFrom-Json
$productIdNum = [int]$productObj.productId
$productInDb = [int](docker exec ps3-db-1 psql -U user -d write_db -tAc "SELECT COUNT(*) FROM products WHERE id = $productIdNum;")
$req6Pass = ($productRes.StatusCode -eq 201 -and $productIdNum -gt 0 -and $productInDb -eq 1)
Add-Result 6 'Create product endpoint' $req6Pass "Status=$($productRes.StatusCode), productId=$productIdNum"

# 7) POST /api/orders
$orderBody1 = @{ customerId = 3333; items = @(@{ productId = $productIdNum; quantity = 2; price = 100.0 }) } | ConvertTo-Json -Depth 5
$orderRes1 = Invoke-WebRequest -Method Post -Uri 'http://localhost:8080/api/orders' -ContentType 'application/json' -Body $orderBody1 -UseBasicParsing
$orderObj1 = $orderRes1.Content | ConvertFrom-Json
$orderId1 = [int]$orderObj1.orderId
$orderRows = [int](docker exec ps3-db-1 psql -U user -d write_db -tAc "SELECT (SELECT COUNT(*) FROM orders WHERE id=$orderId1) + (SELECT COUNT(*) FROM order_items WHERE order_id=$orderId1);")
$req7Pass = ($orderRes1.StatusCode -eq 201 -and $orderId1 -gt 0 -and $orderRows -eq 2)
Add-Result 7 'Create order endpoint' $req7Pass "Status=$($orderRes1.StatusCode), orderId=$orderId1"

# 8) Outbox event created and initially unpublished
$lastOutbox = docker exec ps3-db-1 psql -U user -d write_db -tAc "SELECT topic || '|' || (payload->>'eventType') || '|' || CASE WHEN published_at IS NULL THEN 'NULL' ELSE 'NOT_NULL' END FROM outbox ORDER BY id DESC LIMIT 1;"
$req8Pass = ($lastOutbox -match '^order-events\|OrderCreated\|NULL$')
Add-Result 8 'OrderCreated outbox entry' $req8Pass $lastOutbox

# Add another order for stronger aggregation checks
$orderBody2 = @{ customerId = 3333; items = @(@{ productId = $productIdNum; quantity = 1; price = 100.0 }) } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/orders' -ContentType 'application/json' -Body $orderBody2 | Out-Null
Start-Sleep -Seconds 8

# 9) product_sales_view columns
$r9 = [int](docker exec ps3-db-1 psql -U user -d read_db -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='product_sales_view' AND column_name IN ('product_id','total_quantity_sold','total_revenue','order_count');")
Add-Result 9 'product_sales_view columns' ($r9 -eq 4) "Found $r9/4 required columns"

# 10) category_metrics_view columns
$r10 = [int](docker exec ps3-db-1 psql -U user -d read_db -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='category_metrics_view' AND column_name IN ('category_name','total_revenue','total_orders');")
Add-Result 10 'category_metrics_view columns' ($r10 -eq 3) "Found $r10/3 required columns"

# 11) customer_ltv_view columns
$r11 = [int](docker exec ps3-db-1 psql -U user -d read_db -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='customer_ltv_view' AND column_name IN ('customer_id','total_spent','order_count','last_order_date');")
Add-Result 11 'customer_ltv_view columns' ($r11 -eq 4) "Found $r11/4 required columns"

# 12) hourly_sales_view columns
$r12 = [int](docker exec ps3-db-1 psql -U user -d read_db -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='hourly_sales_view' AND column_name IN ('hour_timestamp','total_orders','total_revenue');")
Add-Result 12 'hourly_sales_view columns' ($r12 -eq 3) "Found $r12/3 required columns"

# 13) Product sales analytics endpoint
$g13 = Invoke-WebRequest -Method Get -Uri ("http://localhost:8081/api/analytics/products/{0}/sales" -f $productIdNum) -UseBasicParsing
$j13 = $g13.Content | ConvertFrom-Json
$req13Pass = ($g13.StatusCode -eq 200 -and [int]$j13.totalQuantitySold -eq 3 -and [double]$j13.totalRevenue -eq 300 -and [int]$j13.orderCount -eq 2)
Add-Result 13 'GET product sales' $req13Pass $g13.Content

# 14) Category revenue analytics endpoint
$g14 = Invoke-WebRequest -Method Get -Uri 'http://localhost:8081/api/analytics/categories/verify-cat/revenue' -UseBasicParsing
$j14 = $g14.Content | ConvertFrom-Json
$req14Pass = ($g14.StatusCode -eq 200 -and [double]$j14.totalRevenue -eq 300 -and [int]$j14.totalOrders -eq 2)
Add-Result 14 'GET category revenue' $req14Pass $g14.Content

# 15) Customer LTV endpoint
$g15 = Invoke-WebRequest -Method Get -Uri 'http://localhost:8081/api/analytics/customers/3333/lifetime-value' -UseBasicParsing
$j15 = $g15.Content | ConvertFrom-Json
$req15Pass = ($g15.StatusCode -eq 200 -and [double]$j15.totalSpent -eq 300 -and [int]$j15.orderCount -eq 2 -and $null -ne $j15.lastOrderDate)
Add-Result 15 'GET customer LTV' $req15Pass $g15.Content

# 16) Sync status updates after new event
$syncBefore = Invoke-RestMethod -Method Get -Uri 'http://localhost:8081/api/analytics/sync-status'
$syncProduct = Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/products' -ContentType 'application/json' -Body '{"name":"Sync Product","category":"sync-cat","price":10,"stock":10}'
$syncProductIdNum = [int]$syncProduct.productId
$syncOrderBody = @{ customerId = 7777; items = @(@{ productId = $syncProductIdNum; quantity = 1; price = 10.0 }) } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/orders' -ContentType 'application/json' -Body $syncOrderBody | Out-Null
Start-Sleep -Seconds 5
$syncAfter = Invoke-RestMethod -Method Get -Uri 'http://localhost:8081/api/analytics/sync-status'
$req16Pass = ($null -ne $syncAfter.lastProcessedEventTimestamp -and [double]$syncAfter.lagSeconds -ge 0 -and $syncBefore.lastProcessedEventTimestamp -ne $syncAfter.lastProcessedEventTimestamp)
Add-Result 16 'GET sync-status update' $req16Pass ((ConvertTo-Json @{before=$syncBefore; after=$syncAfter} -Compress))

# Extra negative tests
try {
    Invoke-WebRequest -Method Post -Uri 'http://localhost:8080/api/products' -ContentType 'application/json' -Body '{"name":"","category":"x","price":-1,"stock":-1}' -UseBasicParsing | Out-Null
    Add-Result 101 'Negative: invalid product payload' $false 'Expected 400 but request succeeded'
} catch {
    $status = $_.Exception.Response.StatusCode.value__
    Add-Result 101 'Negative: invalid product payload' ($status -eq 400) "Status=$status"
}

try {
    $unknownOrder = @{ customerId = 42; items = @(@{ productId = 999999; quantity = 1; price = 10.0 }) } | ConvertTo-Json -Depth 5
    Invoke-WebRequest -Method Post -Uri 'http://localhost:8080/api/orders' -ContentType 'application/json' -Body $unknownOrder -UseBasicParsing | Out-Null
    Add-Result 102 'Negative: unknown product order' $false 'Expected 400 but request succeeded'
} catch {
    $status = $_.Exception.Response.StatusCode.value__
    Add-Result 102 'Negative: unknown product order' ($status -eq 400) "Status=$status"
}

try {
    $low = Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/products' -ContentType 'application/json' -Body '{"name":"LowStockVerify","category":"neg","price":5,"stock":1}'
    $lowId = [int]$low.productId
    $tooMuch = @{ customerId = 43; items = @(@{ productId = $lowId; quantity = 2; price = 5.0 }) } | ConvertTo-Json -Depth 5
    Invoke-WebRequest -Method Post -Uri 'http://localhost:8080/api/orders' -ContentType 'application/json' -Body $tooMuch -UseBasicParsing | Out-Null
    Add-Result 103 'Negative: insufficient stock' $false 'Expected 409 but request succeeded'
} catch {
    $status = $_.Exception.Response.StatusCode.value__
    Add-Result 103 'Negative: insufficient stock' ($status -eq 409) "Status=$status"
}

# Report
$results | Sort-Object Requirement | Format-Table -AutoSize

$coreFails = ($results | Where-Object { $_.Requirement -ge 1 -and $_.Requirement -le 16 -and $_.Status -eq 'FAIL' }).Count
$extraFails = ($results | Where-Object { $_.Requirement -gt 100 -and $_.Status -eq 'FAIL' }).Count

Write-Host ''
Write-Host ("Core Score: " + (16 - $coreFails) + "/16") -ForegroundColor Yellow
Write-Host ("Extra Negative Tests: " + (3 - $extraFails) + "/3") -ForegroundColor Yellow

if ($coreFails -eq 0 -and $extraFails -eq 0) {
    Write-Host 'Overall: PASS' -ForegroundColor Green
    exit 0
} elseif ($coreFails -eq 0) {
    Write-Host 'Overall: PASS (core), FAIL (extras)' -ForegroundColor DarkYellow
    exit 0
} else {
    Write-Host 'Overall: FAIL' -ForegroundColor Red
    exit 1
}
