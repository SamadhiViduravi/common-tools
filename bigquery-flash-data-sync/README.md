# BigQuery Flash Data Sync

A Go application that synchronizes MySQL databases (sales and finance data) to Google Cloud BigQuery with automatic schema inference and concurrent processing.

## 📋 Quick Start

### Prerequisites

- Go 1.21+
- Google Cloud SDK with BigQuery API enabled
- MySQL 5.7+ with read access
- Service account with `bigquery.dataEditor` and `bigquery.jobUser` roles

### Installation

```bash
# Clone and navigate
cd digiops-finance/operations/bigquery-flash-data-sync

# Install dependencies
go mod download

# Set up authentication
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# Configure environment
cp .env.example .env
# Edit .env with your credentials

# Build
go build -o bin/datasync cmd/datasync/main.go
```

### Configuration

Copy `.env.example` to `.env` and configure:

```bash
# Required
GCP_PROJECT_ID=your_gcp_project_id

BQ_DATASET_ID=your_bq_dataset
DB_HOST=db.example.com:3306

FINANCE_DB_NAME=finance_db
FINANCE_DB_USER=finance_user
FINANCE_DB_PASSWORD=password

SALESFORCE_DB_NAME=salesforce_db
SALESFORCE_DB_USER=sales_user
SALESFORCE_DB_PASSWORD=password

DB_MAX_OPEN_CONNECTIONS=20
DB_MAX_IDLE_CONNECTIONS=10

SYNC_TIMEOUT=10m

DB_CONN_MAX_LIFETIME=1m

DATE_FORMAT=2006-01-02T15:04:05Z07:00 # or DATE_FORMAT=YYYY-MM-DD

LOG_ENV=dev #or prod
LOG_LEVEL=info
```

See [`.env.example`](.env.example) for detailed documentation.

### Run

```bash
# Development
LOG_ENV=dev ./bin/datasync

# Production
./bin/datasync
```

## 🏗 How It Works

1. **Schema Inference**: Automatically detects MySQL schemas and maps to BigQuery types
2. **Concurrent Extraction**: Parallel processing of multiple tables
3. **Data Sanitization**: Handles special characters, NULLs, and invalid UTF-8
4. **BigQuery Loading**: Creates/updates tables and loads data with `WRITE_TRUNCATE`

## 📁 Project Structure

```
bigquery-flash-data-sync/
├── README.md
├── .env.example                 # Configuration template
├── go.mod                       # Go module dependencies
├── go.sum                       # Dependency checksums
├── assets/
│   └── schemas/                 # Schema documentation
├── cmd/
│   └── datasync/
│       └── main.go              # Application entry point
└── internal/
    ├── config/
    │   └── config.go            # Environment variable loading
    ├── logger/
    │   └── logger.go            # Logging configuration
    ├── model/
    │   ├── models.go            # Data structures
    │   └── parser.go            # Row parsing & type conversion
    └── pipeline/
        ├── bqsetup.go           # Schema inference & table management
        └── job.go               # ETL job orchestration
```

## 🔧 Adding Tables

Edit `internal/pipeline/job.go`:

```go
sources := []model.DataSource{
    {
        Name:   "salesforce",
        DSN:    cfg.SalesforceDBConnString,
        DBName: cfg.SalesforceDBName,
        SourceTables: []string{
            "arr_sf_opportunity",
            "arr_sf_account",
            "new_table_here",  // Add your table
        },
    },
}
```

Schema detection is automatic - just ensure your DB user has `SELECT` permission.

## 🐛 Troubleshooting

**Connection Issues**

```bash
# Verify DB connectivity
mysql -h $DB_HOST -u $FINANCE_DB_USER -p

# Check BigQuery access
bq ls --project_id=$GCP_PROJECT_ID
```

**Enable Debug Logging**

```bash
LOG_ENV=dev ./bin/datasync
```

**Common Errors**

| Error                           | Solution                                       |
| ------------------------------- | ---------------------------------------------- |
| `dial tcp: i/o timeout`         | Check `DB_HOST` includes port, verify firewall |
| `Access denied`                 | Verify credentials, check user permissions     |
| `Permission denied` (BigQuery)  | Add required IAM roles to service account      |
| `JSON table encountered errors` | Enable debug mode, check for invalid UTF-8     |

## 📊 Performance

| Rows | Columns | Sync Time | Memory |
| ---- | ------- | --------- | ------ |
| 1K   | 10      | ~2s       | ~50MB  |
| 50K  | 25      | ~15s      | ~200MB |
| 500K | 50      | ~90s      | ~800MB |

**Optimization**: Adjust `DB_MAX_OPEN_CONNS` and `SYNC_TIMEOUT` based on data volume.

## 🔒 Security

- Never commit `.env` to version control
- Use read-only database users
- Store production credentials in secret manager
- Enable TLS for all connections (automatically configured)
- Review [`.env.example`](.env.example) for security best practices

## 📝 License

Copyright (c) 2025, WSO2 LLC. All Rights Reserved.
This software is the property of WSO2 LLC. and its suppliers, if any.
Dissemination of any information or reproduction of any material contained
herein in any form is strictly forbidden, unless permitted by WSO2 expressly.
You may not alter or remove any copyright or other notice from copies of this content.

---

**Maintained by**: WSO2 Internal Apps Team
