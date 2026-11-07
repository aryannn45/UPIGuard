# Publish UPIGuard to GitHub

The repository is prepared locally on the `main` branch. Do not create the
remote repository, commit, or push until the owner has reviewed the staged
file list and explicitly approved publishing.

## Before committing

From the project directory, confirm the local environment file is ignored and
review the complete staged filename list:

```powershell
git status --short --branch
git check-ignore -v .env
git diff --cached --name-only
git diff --cached --check
```

`.env`, `.env.local`, `.env.production`, `.env.development`, and other local
environment files are excluded. `.env.example` is safe to publish only while
its values remain placeholders. Never force-add an ignored environment file.

## After approval

Create a **new** GitHub repository named `UPIGuard` under your connected
account. Use this description:

> Real-Time UPI Fraud Detection and Risk Analytics using Apache Kafka, Spark
> Structured Streaming, Machine Learning, TimescaleDB and Grafana.

Make it public unless you choose private. Create it empty; do not initialize it
with a README, license, or `.gitignore`, since those files are already present.
Do not overwrite or repurpose an existing repository.

Then verify the remote URL and publish `main`:

```powershell
git remote add origin https://github.com/aryannn45/UPIGuard.git
git remote -v
git push -u origin main
```
GitHub may ask you to authenticate through the browser or Git Credential
Manager.

Keep the upstream MIT `LICENSE` and `NOTICE.md` attribution in the repository.
