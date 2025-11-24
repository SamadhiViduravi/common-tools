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

// Package model contains all data structures, BigQuery schemas, and data transformation methods.
// It acts as the "model" layer for the ETL pipeline.
package model

import (
	"database/sql"
	"time"

	"cloud.google.com/go/bigquery"
	"go.uber.org/zap"
)

type Savable interface {
	ToSaveable() map[string]any
}

type BQTable struct {
	Name   string
	Schema bigquery.Schema
}

type DynamicRow struct {
	ColumnNames []string
	Values      []any
}

type DataSource struct {
	Name         string
	DSN          string
	DBName       string
	SourceTables []string
}

type Job struct {
	Name             string
	ConnectionString string
	Query            string
	BigQueryTable    string
	ParseFunc        func(*sql.Rows, *zap.Logger) (Savable, error)
}

type Config struct {
	GCPProjectID           string
	BigQueryDatasetID      string
	SalesforceDBConnString string
	SalesforceDBName       string
	FinanceDBConnString    string
	FinanceDBName          string
	SyncTimeout            time.Duration
	MaxOpenConns           int
	MaxIdleConns           int
	DateFormat             string
	ConnMaxLifetime        time.Duration
}

// SchemasMatch validates equality of two BigQuery schemas by comparing field names and types.
// Records debug and warning logs for field count differences and type mismatches.
// Returns true only if no missing or mismatched fields are detected.
func SchemasMatch(s1, s2 bigquery.Schema, logger *zap.Logger) bool {
	logger.Debug("Comparing BigQuery schemas",
		zap.Int("schema1_fields", len(s1)),
		zap.Int("schema2_fields", len(s2)))
	if len(s1) != len(s2) {
		logger.Warn("Schema field count mismatch",
			zap.Int("schema1_count", len(s1)),
			zap.Int("schema2_count", len(s2)))
		return false
	}
	mapS1 := make(map[string]bigquery.FieldType)
	for _, field := range s1 {
		mapS1[field.Name] = field.Type
	}
	mismatches := []string{}
	for _, field := range s2 {
		if t, ok := mapS1[field.Name]; !ok {
			mismatches = append(mismatches, "missing:"+field.Name)
		} else if t != field.Type {
			mismatches = append(mismatches, "type:"+field.Name)
		}
	}
	if len(mismatches) > 0 {
		logger.Warn("Schema mismatches detected",
			zap.Strings("mismatches", mismatches))
		return false
	}
	logger.Debug("Schemas match")
	return true
}

// ToSaveable converts a DynamicRow into a map representation using column names as keys.
// Iterates through all columns and assigns corresponding values from the row.
// Returns a generic map[string]any suitable for serialization or database storage.
func (r *DynamicRow) ToSaveable() map[string]any {
	result := make(map[string]any)
	for i, colName := range r.ColumnNames {
		result[colName] = r.Values[i]
	}
	return result
}
