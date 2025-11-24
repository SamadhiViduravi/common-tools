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

// Package config is responsible for loading and parsing all environment variables
// needed for the application to run.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/wso2-enterprise/digiops-finance/bigquery-flash-data-sync/internal/model"
	"go.uber.org/zap"
)

const (
	DBHost            = "DB_HOST"
	FinanceDBName     = "FINANCE_DB_NAME"
	FinanceDBUser     = "FINANCE_DB_USER"
	FinanceDBPassword = "FINANCE_DB_PASSWORD"

	SalesforceDBName = "SALESFORCE_DB_NAME"
	SalesforceDBUser = "SALESFORCE_DB_USER"
	SalesforceDBPass = "SALESFORCE_DB_PASSWORD"

	GCPProjectID = "GCP_PROJECT_ID"
	BQDatasetID  = "BQ_DATASET_ID"

	DBMaxOpenConns    = "DB_MAX_OPEN_CONNECTIONS"
	DBMaxIdleConns    = "DB_MAX_IDLE_CONNECTIONS"
	DBConnMaxLifetime = "DB_CONN_MAX_LIFETIME"

	SyncTimeoutKey = "SYNC_TIMEOUT"
	DateFormatKey  = "DATE_FORMAT"
)

// LoadConfig reads all required environment variables and builds MySQL connection strings.
// It returns a populated Config struct or an error if any required variable is missing.
func LoadConfig(logger *zap.Logger) (*model.Config, error) {
	logger.Info("Loading configuration from environment variables")

	requiredVars := map[string]string{
		DBHost:            getEnv(DBHost, "localhost"),
		FinanceDBName:     getEnv(FinanceDBName, "finance_db"),
		FinanceDBUser:     getEnv(FinanceDBUser, "finance_user"),
		FinanceDBPassword: getEnv(FinanceDBPassword, "password"),
		SalesforceDBName:  getEnv(SalesforceDBName, "salesforce_db"),
		SalesforceDBUser:  getEnv(SalesforceDBUser, "salesforce_user"),
		SalesforceDBPass:  getEnv(SalesforceDBPass, "password"),
		GCPProjectID:      getEnv(GCPProjectID, "finance-mis"),
		BQDatasetID:       getEnv(BQDatasetID, "finance_salesforce"),
		DateFormatKey:     getEnv(DateFormatKey, "2006-01-02T15:04:05Z07:00"),
	}

	missingVars := []string{}
	for key, value := range requiredVars {
		if strings.TrimSpace(value) == "" {
			missingVars = append(missingVars, key)
		}
	}

	if len(missingVars) > 0 {
		logger.Error("Missing required environment variables",
			zap.Strings("missing_vars", missingVars))
		return nil, fmt.Errorf("missing required environment variables: %v", missingVars)
	}

	financeConnString := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?tls=true&parseTime=true&timeout=30s&readTimeout=60s&writeTimeout=60s",
		requiredVars[FinanceDBUser],
		requiredVars[FinanceDBPassword],
		requiredVars[DBHost],
		requiredVars[FinanceDBName],
	)

	salesforceConnString := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?tls=true&parseTime=true&timeout=30s&readTimeout=60s&writeTimeout=60s",
		requiredVars[SalesforceDBUser],
		requiredVars[SalesforceDBPass],
		requiredVars[DBHost],
		requiredVars[SalesforceDBName],
	)

	maxOpen, err := strconv.Atoi(getEnv(DBMaxOpenConns, "10"))
	if err != nil {
		logger.Warn("Invalid DB_MAX_OPEN_CONNECTIONS, using default 10", zap.Error(err))
		maxOpen = 10
	}

	maxIdle, err := strconv.Atoi(getEnv(DBMaxIdleConns, "10"))
	if err != nil {
		logger.Warn("Invalid DB_MAX_IDLE_CONNECTIONS, using default 10", zap.Error(err))
		maxIdle = 10
	}

	syncTimeout := 10 * time.Minute
	connMaxLifetime := 0 * time.Second

	v := getEnv(SyncTimeoutKey, "10m")
	d, err := time.ParseDuration(v)
	if err != nil {
		logger.Warn("Invalid SYNC_TIMEOUT, using default 10m",
			zap.String("value", v), zap.Error(err))
	} else {
		syncTimeout = d
	}

	v = getEnv(DBConnMaxLifetime, "1m")
	d, err = time.ParseDuration(v)
	if err != nil {
		logger.Warn("Invalid DB_CONN_MAX_LIFETIME, using 0s",
			zap.String("value", v), zap.Error(err))
	} else {
		connMaxLifetime = d
	}

	cfg := &model.Config{
		GCPProjectID:           requiredVars[GCPProjectID],
		BigQueryDatasetID:      requiredVars[BQDatasetID],
		SalesforceDBConnString: salesforceConnString,
		SalesforceDBName:       requiredVars[SalesforceDBName],
		FinanceDBConnString:    financeConnString,
		FinanceDBName:          requiredVars[FinanceDBName],
		SyncTimeout:            syncTimeout,
		DateFormat:             requiredVars[DateFormatKey],
		MaxOpenConns:           maxOpen,
		MaxIdleConns:           maxIdle,
		ConnMaxLifetime:        connMaxLifetime,
	}

	logger.Info("Configuration loaded successfully",
		zap.String("gcp_project", cfg.GCPProjectID),
		zap.String("bq_dataset", cfg.BigQueryDatasetID),
	)
	return cfg, nil
}

// getEnv retrieves an environment variable by key, returning defaultValue if the variable is not set or empty.
// This helper function provides a convenient way to load optional configuration with fallback values.
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
