# UCM6202 CRM

Docker Compose CRM for Grandstream UCM6202.

Features in this build:
- Automatic CDR sync every 60 seconds
- Manual UCM sync button
- Correct inbound/outbound/internal classification using Grandstream CDR `userfield` and trunk fields
- Grouped CDR session handling (`main_cdr` + `sub_cdr_*`)
- Dashboard statistics
- Recent calls
- Contacts and customer history
- Recording API plumbing
- Click-to-Call API placeholder for UCM control/AMI

## UCM `.env`

```env
UCM_CDR_URL=https://192.168.10.10:8443/cdrapi
UCM_REC_URL=https://192.168.10.10:8443/recapi
UCM_USER=cdrapi
UCM_PASS=cdrapi123
UCM_VERIFY_TLS=false
DATABASE_URL=postgresql+psycopg://crm:crm123@db:5432/ucmcrm
SYNC_INTERVAL_SECONDS=60
```

Do not use `docker compose down -v` unless you intentionally want to delete PostgreSQL data.
# ucm6202-crm
