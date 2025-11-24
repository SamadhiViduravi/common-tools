// Copyright (c) 2025 WSO2 LLC. (https://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.ent.

// Package pipeline provides utilities to orchestrate ETL jobs that extract data from MySQL sources,
// infer schemas, and load the data into BigQuery tables concurrently with logging and error handling.
package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/wso2-enterprise/digiops-finance/bigquery-flash-data-sync/internal/model"
	"golang.org/x/sync/errgroup"

	"cloud.google.com/go/bigquery"
	"go.uber.org/zap"
)

// Start initializes the BigQuery client and orchestrates multiple concurrent ETL jobs using errgroup.
// It connects to different data sources, infers schemas, updates BigQuery tables, and runs extraction jobs.
// Returns an error if any job fails or if client initialization encounters an issue.
func Start(ctx context.Context, cfg *model.Config, logger *zap.Logger) error {
	bqClient, err := bigquery.NewClient(ctx, cfg.GCPProjectID)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery client: %w", err)
	}
	defer bqClient.Close()

	sources := []model.DataSource{
		{
			Name:   "salesforce",
			DSN:    cfg.SalesforceDBConnString,
			DBName: cfg.SalesforceDBName,
			SourceTables: []string{
				"arr_sf_opportunity",
				"arr_sf_account",
			},
		},
		{
			Name:   "finance",
			DSN:    cfg.FinanceDBConnString,
			DBName: cfg.FinanceDBName,
			SourceTables: []string{
				"cache_financial_acc_expense",
				"cache_financial_acc_cost_of_sales",
				"cache_financial_acc_income",
			},
		},
	}

	g, ctx := errgroup.WithContext(ctx)

	for _, src := range sources {
		source := src
		for _, tableName := range source.SourceTables {
			table := tableName
			jobLogger := logger.With(zap.String("job_name", table))

			g.Go(func() error {
				return runTableJob(ctx, bqClient, cfg, source, table, jobLogger)
			})
		}
	}
	// Wait for all jobs to finish or first error
	if err := g.Wait(); err != nil {
		logger.Error("One or more jobs failed", zap.Error(err))
		return err
	}

	logger.Info("All jobs completed successfully")
	return nil
}

// runTableJob handles the ETL process for a single table, including schema inference,
// BigQuery table creation/update, data extraction, and load.
func runTableJob(ctx context.Context, bqClient *bigquery.Client, cfg *model.Config, source model.DataSource, table string, logger *zap.Logger) error {
	logger.Info("Starting concurrent job")

	sourceQuery := fmt.Sprintf("SELECT * FROM %s.%s", source.DBName, table)
	dummyQuery := sourceQuery + " LIMIT 1"

	// Open DB connection for schema inference
	db, err := sql.Open("mysql", source.DSN)
	if err != nil {
		return fmt.Errorf("failed to open DB connection for schema inference: %w", err)
	}

	// Apply connection pool settings
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	defer db.Close()

	// Infer BigQuery schema from MySQL
	inferredSchema, err := InferSchemaFromMySQL(db, dummyQuery, logger)
	if err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	// Create or update BigQuery table
	bqTable := model.BQTable{Name: table, Schema: inferredSchema}
	if err := createOrUpdateTable(ctx, bqClient, cfg.BigQueryDatasetID, bqTable, logger); err != nil {
		return fmt.Errorf("failed to create/update BigQuery table: %w", err)
	}

	// Prepare and execute the ETL job
	job := model.Job{
		Name:             table,
		ConnectionString: source.DSN,
		Query:            sourceQuery,
		BigQueryTable:    table,
		ParseFunc: func(rows *sql.Rows, logger *zap.Logger) (model.Savable, error) {
			return model.ParseDynamicRow(rows, logger, cfg.DateFormat)
		},
	}

	if err := executeJob(ctx, bqClient, cfg.BigQueryDatasetID, job, db, logger); err != nil {
		return fmt.Errorf("job execution failed: %w", err)
	}

	logger.Info("Job completed successfully", zap.String("table", job.BigQueryTable))
	return nil
}

// executeJob runs a full extract-and-load process by querying the source database, buffering results in memory,
// and uploading the extracted JSON data to BigQuery using a load job with truncate semantics.
// Returns an error if querying, parsing, buffering, or BigQuery loading fails at any stage.
func executeJob(ctx context.Context, bqClient *bigquery.Client, datasetID string, job model.Job, db *sql.DB, logger *zap.Logger) error {
	// Ensure db is not nil
	if db == nil {
		return fmt.Errorf("database connection is nil")
	}

	logger.Info("Executing query", zap.String("job_name", job.Name))
	rows, err := db.QueryContext(ctx, job.Query)
	if err != nil {
		logger.Error("Failed to query database", zap.Error(err))
		return fmt.Errorf("failed to query database: %w", err)
	}
	defer rows.Close()

	// In-Memory Buffer
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	logger.Debug("Starting data extraction to in-memory buffer")

	totalRowsExtracted := 0
	rowNum := 0
	skippedRows := 0

	for rows.Next() {
		rowNum++
		row, err := job.ParseFunc(rows, logger)
		if err != nil {
			logger.Error("Failed to parse row",
				zap.Int("row_number", rowNum),
				zap.Error(err),
			)
			zap.Any("raw_row", rows)
			skippedRows++
			continue
		}

		if err := encoder.Encode(row.ToSaveable()); err != nil {
			logger.Error("Failed to write row to memory buffer",
				zap.Int("row_number", rowNum),
				zap.Error(err),
			)
			return fmt.Errorf("failed to write row to memory buffer: %w", err)
		}

		totalRowsExtracted++
	}

	if err := rows.Err(); err != nil {
		logger.Error("Error during row iteration", zap.Error(err))
		return fmt.Errorf("error during row iteration: %w", err)
	}

	logger.Info("Extraction complete",
		zap.Int("rows_extracted", totalRowsExtracted),
		zap.Int("rows_skipped", skippedRows),
	)

	if skippedRows > 0 {
		logger.Warn("Some rows were skipped during parsing",
			zap.Int("skipped_rows", skippedRows),
		)
	}

	if totalRowsExtracted == 0 {
		logger.Info("No rows to load. Job finished.")
		return nil
	}

	source := bigquery.NewReaderSource(&buf)
	source.SourceFormat = bigquery.JSON
	loader := bqClient.Dataset(datasetID).Table(job.BigQueryTable).LoaderFrom(source)
	loader.WriteDisposition = bigquery.WriteTruncate

	logger.Info("Starting BigQuery load job", zap.String("table", job.BigQueryTable))
	bqJob, err := loader.Run(ctx)
	if err != nil {
		logger.Error("Failed to create BigQuery load job", zap.Error(err))
		return fmt.Errorf("failed to create BigQuery load job: %w", err)
	}
	status, err := bqJob.Wait(ctx)
	if err != nil {
		logger.Error("Failed to wait for BigQuery job to complete", zap.Error(err))
		return fmt.Errorf("failed to wait for BigQuery job to complete: %w", err)
	}
	if err := status.Err(); err != nil {
		logger.Error("BigQuery load job failed", zap.Error(err))
		return fmt.Errorf("BigQuery load job failed: %w", err)
	}
	logger.Info("BigQuery load job completed successfully", zap.String("table", job.BigQueryTable))
	return nil
}
