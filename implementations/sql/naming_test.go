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
				Columns: justColumns("id", "name", "column_12b241e808ae6c964a5bb9f1c012e63d", "1test_name", "2", "column_c16da609b86c01f16a2c609eac4ccb0c", "Test Name", "Test Name DROP DATABASE public SELECT 1 from DUAL", "Université Français", "_timestamp", "_unnamed", "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit_sed_do_e", "Странное Имя", "秒速センチメートル", "camelCase", "int", "user", "select", "__ROOT__"),
			},
			configIds: utils.ArrayExcluding(allBulkerConfigs, RedshiftBulkerTypeId+"_serverless", RedshiftBulkerTypeId, SnowflakeBulkerTypeId, BigqueryBulkerTypeId),
		},
		{
			name:              "naming_test1_case_redshift",
			tableName:         "Strange Table Name; DROP DATABASE public;",
			modes:             []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			dataFile:          "test_data/identifiers.ndjson",
			expectPartitionId: true,
			// Redshift is case insensitive by default. During creation it changes all created identifiers to lowercase.
			// Case sensitivity may be enabled on the server side or for a session: https://docs.aws.amazon.com/redshift/latest/dg/r_enable_case_sensitive_identifier.html
			// TODO: consider 'case sensitivity' option for bulker stream
			expectedTableCaseChecking: false,
			expectedRowsCount:         1,
			expectedTable: ExpectedTable{
				Columns: justColumns("id", "name", "column_12b241e808ae6c964a5bb9f1c012e63d", "1test_name", "2", "column_c16da609b86c01f16a2c609eac4ccb0c", "Test Name", "Test Name DROP DATABASE public SELECT 1 from DUAL", "Université Français", "_timestamp", "_unnamed", "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit_sed_do_eiusmod_tempor_incididunt_ut_labore_et_dolore_magna_aliqua_ut_eni", "Странное Имя", "秒速センチメートル", "camelCase", "int", "user", "select", "__ROOT__"),
				//Columns: justColumns("id", "name", "column_12b241e808ae6c964a5bb9f1c012e63d", "1test_name", "2", "column_c16da609b86c01f16a2c609eac4ccb0c", "test name", "test name drop database public select 1 from dual", "université français", "_timestamp", "_unnamed", "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit_sed_do_eiusmod_tempor_incididunt_ut_labore_et_dolore_magna_aliqua_ut_eni", "странное имя", "秒速センチメートル", "camelcase", "int", "user", "select","__root__"),
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
			orderBy:                   "\"id\" asc",
			expectedRowsCount:         1,
			expectedTable: ExpectedTable{
				Columns: justColumns("id", "name", "column_12b241e808ae6c964a5bb9f1c012e63d", "1test_name", "2", "column_c16da609b86c01f16a2c609eac4ccb0c", "Test Name", "Test Name DROP DATABASE public SELECT 1 from DUAL", "Université Français", "_timestamp", "_unnamed", "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit_sed_do_eiusmod_tempor_incididunt_ut_labore_et_dolore_magna_aliqua_ut_enim_ad_minim_veniam_quis_nostrud_exercitation_ullamco_laboris_nisi_ut_aliquip_ex_ea_commodo_consequat", "Странное Имя", "秒速センチメートル", "camelCase", "int", "user", "select", "__ROOT__"),
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
				Columns: justColumns("id", "name", "column_12b241e808ae6c964a5bb9f1c012e63d", "_1test_name", "_2", "column_c16da609b86c01f16a2c609eac4ccb0c", "Test_Name", "Test_Name_DROP_DATABASE_public_SELECT_1_from_DUAL", "Universit_Franais", "_timestamp", "_unnamed", "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit_sed_do_eiusmod_tempor_incididunt_ut_labore_et_dolore_magna_aliqua_ut_enim_ad_minim_veniam_quis_nostrud_exercitation_ullamco_laboris_nisi_ut_aliquip_ex_ea_commodo_consequat", "column_c41d0d6c9ff6db34c6df393bdd283e19", "column_8aecb803355d27292f63cb2b94b44c5b", "camelCase", "int", "user", "select", "___ROOT__"),
			},
			configIds: []string{BigqueryBulkerTypeId},
		},
		{
			name:              "naming_test2",
			tableName:         "Université Français",
			modes:             []bulker.BulkMode{bulker.Transactional},
			dataFile:          "test_data/simple.ndjson",
			orderBy:           "\"id\" asc",
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
