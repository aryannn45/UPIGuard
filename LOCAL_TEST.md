# Test UPIGuard locally without stopping another Docker project

Use this alternate Compose file if another project is already using the default host ports (2181, 9092, 5432, 3000, 8080, or 8081).

1. Create `.env` from `.env.example` and set local-only passwords.
2. In PowerShell, from this directory, run:

   ```powershell
   docker compose -f docker-compose.local-test.yml up --build -d
   ```

3. Check services:

   ```powershell
   docker compose -f docker-compose.local-test.yml ps
   ```

4. Watch the streaming job:

   ```powershell
   docker compose -f docker-compose.local-test.yml logs -f spark-streaming
   ```

5. Open Grafana at <http://localhost:3001>. Use the Grafana username and password in `.env`.
   Spark master UI: <http://localhost:8083>.

The local-test Compose project uses a separate project name, container names, volumes, and host ports. It does not require taking down the other project. To stop only this test stack, press Ctrl+C in the log view, then run:

```powershell
docker compose -f docker-compose.local-test.yml down
```

Do not add `-v` unless you intentionally want to delete this test stack's database and Grafana data volumes.
