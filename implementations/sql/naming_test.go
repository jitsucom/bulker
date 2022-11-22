package sql

import (
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"testing"
)

func TestNaming(t *testing.T) {
	t.Parallel()
	tests := []bulkerTestConfig{
		{
			name:                      "naming_test1",
			tableName:                 "Strange Table Name; DROP DATABASE public;",
			modes:                     []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			dataFile:                  "test_data/identifiers.ndjson",
			expectPartitionId:         true,
			expectedRowsCount:         1,
			expectedTableCaseChecking: true,
			expectedTable: ExpectedTable{
				Columns: justColumns("id", "name", "column_12b241e808ae6c964a5bb9f1c012e63d", "1test_name", "2", "column_c16da609b86c01f16a2c609eac4ccb0c", "Test Name", "Test Name DROP DATABASE public SELECT 1 from DUAL", "Université Français", "_timestamp", "_unnamed", "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit_sed_do_e", "Странное Имя", "秒速センチメートル", "camelCase"),
			},
			configIds: utils.ArrayExcluding(allBulkerConfigs, RedshiftBulkerTypeId+"_serverless", RedshiftBulkerTypeId, SnowflakeBulkerTypeId, BigqueryBulkerTypeId),
		},
		{
			//Redshift is case insensitive and supports identifiers up to 127 bytes length
			name:                      "naming_test1_case_redshift",
			tableName:                 "Strange Table Name; DROP DATABASE public;",
			modes:                     []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			dataFile:                  "test_data/identifiers.ndjson",
			expectPartitionId:         true,
			expectedTableCaseChecking: true,
			expectedRowsCount:         1,
			expectedTable: ExpectedTable{
				Columns: justColumns("id", "name", "column_12b241e808ae6c964a5bb9f1c012e63d", "1test_name", "2", "column_c16da609b86c01f16a2c609eac4ccb0c", "test name", "test name drop database public select 1 from dual", "université français", "_timestamp", "_unnamed", "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit_sed_do_eiusmod_tempor_incididunt_ut_labore_et_dolore_magna_aliqua_ut_eni", "странное имя", "秒速センチメートル", "camelcase"),
			},
			configIds: []string{RedshiftBulkerTypeId + "_serverless", RedshiftBulkerTypeId},
		},
		{
			name:                      "naming_test1_case_snowflake",
			tableName:                 "Strange Table Name; DROP DATABASE public;",
			modes:                     []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable},
			dataFile:                  "test_data/identifiers.ndjson",
			expectPartitionId:         true,
			expectedTableCaseChecking: true,
			expectedRowsCount:         1,
			expectedTable: ExpectedTable{
				Columns: justColumns("ID", "NAME", "COLUMN_12B241E808AE6C964A5BB9F1C012E63D", "1test_name", "2", "COLUMN_C16DA609B86C01F16A2C609EAC4CCB0C", "Test Name", "Test Name DROP DATABASE public SELECT 1 from DUAL", "Université Français", "_TIMESTAMP", "_UNNAMED", "LOREM_IPSUM_DOLOR_SIT_AMET_CONSECTETUR_ADIPISCING_ELIT_SED_DO_EIUSMOD_TEMPOR_INCIDIDUNT_UT_LABORE_ET_DOLORE_MAGNA_ALIQUA_UT_ENIM_AD_MINIM_VENIAM_QUIS_NOSTRUD_EXERCITATION_ULLAMCO_LABORIS_NISI_UT_ALIQUIP_EX_EA_COMMODO_CONSEQUAT", "Странное Имя", "秒速センチメートル", "camelCase"),
			},
			configIds: []string{SnowflakeBulkerTypeId},
		},
		{
			name:                      "naming_test1_case_bigquery",
			tableName:                 "Strange Table Name; DROP DATABASE public;",
			modes:                     []bulker.BulkMode{bulker.Transactional, bulker.ReplaceTable, bulker.ReplacePartition},
			dataFile:                  "test_data/identifiers.ndjson",
			expectPartitionId:         true,
			expectedTableCaseChecking: true,
			expectedRowsCount:         1,
			expectedTable: ExpectedTable{
				Columns: justColumns("id", "name", "column_12b241e808ae6c964a5bb9f1c012e63d", "_1test_name", "_2", "column_c16da609b86c01f16a2c609eac4ccb0c", "test_name", "test_name_drop_database_public_select_1_from_dual", "universit_franais", "_timestamp", "_unnamed", "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit_sed_do_eiusmod_tempor_incididunt_ut_labore_et_dolore_magna_aliqua_ut_enim_ad_minim_veniam_quis_nostrud_exercitation_ullamco_laboris_nisi_ut_aliquip_ex_ea_commodo_consequat", "column_c41d0d6c9ff6db34c6df393bdd283e19", "column_8aecb803355d27292f63cb2b94b44c5b", "camelcase"),
			},
			configIds: []string{BigqueryBulkerTypeId},
		},
		{
			name:              "naming_test2",
			tableName:         "Université Français",
			modes:             []bulker.BulkMode{bulker.Transactional},
			dataFile:          "test_data/simple.ndjson",
			expectedRowsCount: 3,
			expectedTable: ExpectedTable{
				Name:    "Université Français_batch",
				Columns: justColumns("id", "name", "_timestamp", "extra"),
			},
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      allBulkerConfigs,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTestConfig(t, tt, testStream)
		})
	}
}
