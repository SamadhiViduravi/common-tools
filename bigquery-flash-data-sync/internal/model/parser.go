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

// Package model contains the data structures for database rows and BigQuery schemas,
// as well as the parser functions required to scan SQL rows into those structs.
package model

import (
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// ParseDynamicRow reads a single SQL row and dynamically maps its columns and values into a DynamicRow.
// It handles scanning, type conversion, and returns a Savable interface for BigQuery ingestion.
func ParseDynamicRow(rows *sql.Rows, logger *zap.Logger, dateFormat string) (Savable, error) {
	columns, err := rows.Columns()
	if err != nil {
		logger.Error("Failed to get columns from query result", zap.Error(err))
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	values := make([]any, len(columns))
	scanArgs := make([]any, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	if err := rows.Scan(scanArgs...); err != nil {
		logger.Error("Failed to scan row values",
			zap.Error(err),
			zap.Int("expected_columns", len(columns)))
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}
	dynamicRow := &DynamicRow{
		ColumnNames: columns,
		Values:      make([]any, len(columns)),
	}
	for i, rawValue := range values {
		dynamicRow.Values[i] = convertValueToInterface(rawValue, dateFormat)
	}
	logger.Debug("Row parsed dynamically", zap.Int("columns_scanned", len(columns)))
	return dynamicRow, nil
}

// convertValueToInterface handles the type conversion from raw SQL driver types
// into types that are safe for JSON encoding and BigQuery loading.
func convertValueToInterface(rawValue any, dateFormat string) any {
	if rawValue == nil {
		return nil
	}
	switch v := rawValue.(type) {
	case []byte:
		return string(v)
	case time.Time:
		return v.Format(dateFormat)
	default:
		return v
	}
}
