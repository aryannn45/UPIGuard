Write-Host "UPIGuard service status" -ForegroundColor Cyan
docker compose ps
Write-Host "`nRecent producer messages" -ForegroundColor Cyan
docker compose logs --tail=10 producer hacker-producer
Write-Host "`nRecent Spark streaming messages" -ForegroundColor Cyan
docker compose logs --tail=20 spark-streaming
Write-Host "`nLatest database rows" -ForegroundColor Cyan
docker compose exec timescaledb sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT transaction_id, transaction_time, city, fraud_prediction, fraud_probability, risk_level FROM transactions ORDER BY transaction_time DESC LIMIT 10;"'
