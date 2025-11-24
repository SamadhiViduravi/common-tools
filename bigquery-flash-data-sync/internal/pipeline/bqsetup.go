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

// Package pipeline provides schema inference, validation, and data-loading utilities
// used to synchronize MySQL source tables with BigQuery in a structured ETL workflow.
package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/wso2-enterprise/digiops-finance/bigquery-flash-data-sync/internal/model"

	"cloud.google.com/go/bigquery"
	"go.uber.org/zap"
)

// mysqlTypeToBigQueryType maps common MySQL database types to BigQuery types.
func mysqlTypeToBigQueryType(mysqlType string, logger *zap.Logger) bigquery.FieldType {
	t := strings.ToUpper(strings.Split(mysqlType, "(")[0])
	switch t {
	case "VARCHAR", "CHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		return bigquery.StringFieldType
	case "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT":
		return bigquery.IntegerFieldType
	case "FLOAT", "DOUBLE", "DECIMAL":
		return bigquery.FloatFieldType
	case "DATE":
		return bigquery.DateFieldType
	case "DATETIME", "TIMESTAMP":
		return bigquery.TimestampFieldType
	case "BOOLEAN", "BOOL":
		return bigquery.BooleanFieldType
	default:
		logger.Warn("Unknown MySQL type, defaulting to STRING",
			zap.String("mysql_type", mysqlType),
			zap.String("default_type", "STRING"))
		return bigquery.StringFieldType
	}
}

// InferSchemaFromMySQL connects to the source DB, runs a LIMIT 1 query,
// and builds a BigQuery Schema based on the returned column types.
func InferSchemaFromMySQL(db *sql.DB, query string, logger *zap.Logger) (bigquery.Schema, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("schema inference query failed: %w", err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types for inference: %w", err)
	}
	logger.Debug("Retrieved column types for schema inference",
		zap.Int("column_count", len(columnTypes)))

	schema := make(bigquery.Schema, 0, len(columnTypes))
	for _, col := range columnTypes {
		bqType := mysqlTypeToBigQueryType(col.DatabaseTypeName(), logger)
		nullable, ok := col.Nullable()
		field := &bigquery.FieldSchema{
			Name:     col.Name(),
			Type:     bqType,
			Required: ok && !nullable,
		}
		schema = append(schema, field)
		logger.Debug("Mapped column to BigQuery field",
			zap.String("column_name", col.Name()),
			zap.String("mysql_type", col.DatabaseTypeName()),
			zap.String("bigquery_type", string(bqType)),
			zap.Bool("required", field.Required))
	}

	logger.Info("Schema inference complete",
		zap.Int("fields_mapped", len(schema)))

	return schema, nil
}

// It ensures that a target table in BigQuery exists and that its schema matches the provided schema.
// If the table does not exist, it is created. If the schema differs, the table schema is updated.
func createOrUpdateTable(ctx context.Context, client *bigquery.Client, datasetID string, table model.BQTable, logger *zap.Logger) error {
	logger.Info("Checking BigQuery table", zap.String("table", table.Name))
	tableRef := client.Dataset(datasetID).Table(table.Name)
	metadata, err := tableRef.Metadata(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "Not found") {
			logger.Info("Table not found, creating new table", zap.String("table", table.Name))
			err = tableRef.Create(ctx, &bigquery.TableMetadata{
				Name:   table.Name,
				Schema: table.Schema,
			})
			if err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}
			logger.Info("Table created successfully", zap.String("table", table.Name))
			return nil
		}
		return fmt.Errorf("failed to get table metadata: %w", err)
	}
	if !model.SchemasMatch(metadata.Schema, table.Schema, logger) {
		logger.Warn("Schema mismatch detected, attempting update",
			zap.String("table", table.Name))
		update := bigquery.TableMetadataToUpdate{Schema: table.Schema}
		_, updateErr := tableRef.Update(ctx, update, metadata.ETag)
		if updateErr != nil {
			isCriticalError := (strings.Contains(updateErr.Error(), "changed type") ||
				strings.Contains(updateErr.Error(), "is missing")) &&
				strings.Contains(updateErr.Error(), "invalid")
			if isCriticalError {
				logger.Error("Critical schema error detected, recreating table",
					zap.String("table", table.Name),
					zap.Error(updateErr))
				logger.Warn("WARNING: Recreating table will DELETE ALL EXISTING DATA",
					zap.String("table", table.Name))
				// Consider: check config flag allowAutoRecreate before proceeding
				if delErr := tableRef.Delete(ctx); delErr != nil {
					return fmt.Errorf("failed to delete table with bad schema: %w", delErr)
				}
				logger.Info("Table deleted", zap.String("table", table.Name))
				if createErr := tableRef.Create(ctx, &bigquery.TableMetadata{
					Name:   table.Name,
					Schema: table.Schema,
				}); createErr != nil {
					return fmt.Errorf("failed to recreate table with correct schema: %w", createErr)
				}
				logger.Info("Table successfully recreated with corrected schema",
					zap.String("table", table.Name))
				return nil
			}
			return fmt.Errorf("failed to update table schema: %w", updateErr)
		}
		logger.Info("Table schema updated successfully", zap.String("table", table.Name))
	} else {
		logger.Debug("Table schema is up to date", zap.String("table", table.Name))
	}
	return nil
}
