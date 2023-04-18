package sql

import (
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"testing"
)

func TestNaming(t *testing.T) {
	t.Parallel()
	tests := []bulkerTestConfig{
		//TODO: enable back ReplaceTable mode when clickhouse driver be patched
		{
			name:                      "naming_test1",
			tableName:                 "Strange Table Name; DROP DATABASE public;",
			modes:                     []bulker.BulkMode{bulker.Batch, bulker.Stream, bulker.ReplaceTable, bulker.ReplacePartition},
			dataFile:                  "test_data/identifiers.ndjson",
			expectPartitionId:         true,
			expectedRowsCount:         1,
			expectedTableCaseChecking: true,
			expectedTable: ExpectedTable{
				Columns: justColumns("id", "name", "column_12b241e808ae6c964a5bb9f1c012e63d", "1test_name", "2", "column_c16da609b86c01f16a2c609eac4ccb0c", "Test Name", "Test Name DROP DATABASE public SELECT 1 from DUAL", "Université Français", "_timestamp", "_unnamed", "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit_sed_do_e", "Странное Имя", "秒速センチメートル", "camelCase", "int", "user", "select", "__ROOT__", "hash"),
			},
			configIds: utils.ArrayExcluding(allBulkerConfigs, RedshiftBulkerTypeId+"_serverless", RedshiftBulkerTypeId, SnowflakeBulkerTypeId, BigqueryBulkerTypeId),
		},
		{
			name:              "naming_test1_case_redshift",
			tableName:         "Strange Table Name; DROP DATABASE public;",
			modes:             []bulker.BulkMode{bulker.Batch, bulker.Stream, bulker.ReplaceTable, bulker.ReplacePartition},
			dataFile:          "test_data/identifiers.ndjson",
			expectPartitionId: true,
			// Redshift is case insensitive by default. During creation it changes all created identifiers to lowercase.
			// Case sensitivity may be enabled on the server side or for a session: https://docs.aws.amazon.com/redshift/latest/dg/r_enable_case_sensitive_identifier.html
			// TODO: consider 'case sensitivity' option for bulker stream
			expectedTableCaseChecking: false,
			expectedRowsCount:         1,
			expectedTable: ExpectedTable{
				Columns: justColumns("id", "name", "column_12b241e808ae6c964a5bb9f1c012e63d", "1test_name", "2", "column_c16da609b86c01f16a2c609eac4ccb0c", "Test Name", "Test Name DROP DATABASE public SELECT 1 from DUAL", "Université Français", "_timestamp", "_unnamed", "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit_sed_do_eiusmod_tempor_incididunt_ut_labore_et_dolore_magna_aliqua_ut_eni", "Странное Имя", "秒速センチメートル", "camelCase", "int", "user", "select", "__ROOT__", "hash"),
				//Columns: justColumns("id", "name", "column_12b241e808ae6c964a5bb9f1c012e63d", "1test_name", "2", "column_c16da609b86c01f16a2c609eac4ccb0c", "test name", "test name drop database public select 1 from dual", "université français", "_timestamp", "_unnamed", "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit_sed_do_eiusmod_tempor_incididunt_ut_labore_et_dolore_magna_aliqua_ut_eni", "странное имя", "秒速センチメートル", "camelcase", "int", "user", "select","__root__"),
			},
			configIds: []string{RedshiftBulkerTypeId + "_serverless", RedshiftBulkerTypeId},
		},
		{
			name:                      "naming_test1_case_snowflake",
			tableName:                 "Strange Table Name; DROP DATABASE public;",
			modes:                     []bulker.BulkMode{bulker.Batch, bulker.Stream, bulker.ReplaceTable},
			dataFile:                  "test_data/identifiers.ndjson",
			expectPartitionId:         true,
			expectedTableCaseChecking: true,
			expectedRowsCount:         1,
			expectedTable: ExpectedTable{
				Columns: justColumns("ID", "NAME", "COLUMN_12B241E808AE6C964A5BB9F1C012E63D", "1test_name", "2", "COLUMN_C16DA609B86C01F16A2C609EAC4CCB0C", "Test Name", "Test Name DROP DATABASE public SELECT 1 from DUAL", "Université Français", "_TIMESTAMP", "_UNNAMED", "LOREM_IPSUM_DOLOR_SIT_AMET_CONSECTETUR_ADIPISCING_ELIT_SED_DO_EIUSMOD_TEMPOR_INCIDIDUNT_UT_LABORE_ET_DOLORE_MAGNA_ALIQUA_UT_ENIM_AD_MINIM_VENIAM_QUIS_NOSTRUD_EXERCITATION_ULLAMCO_LABORIS_NISI_UT_ALIQUIP_EX_EA_COMMODO_CONSEQUAT", "Странное Имя", "秒速センチメートル", "camelCase", "INT", "USER", "SELECT", "__ROOT__", "HASH"),
			},
			configIds: []string{SnowflakeBulkerTypeId},
		},
		{
			name:                      "naming_test1_case_bigquery",
			tableName:                 "Strange Table Name; DROP DATABASE public;",
			modes:                     []bulker.BulkMode{bulker.Batch, bulker.ReplaceTable, bulker.ReplacePartition},
			dataFile:                  "test_data/identifiers.ndjson",
			expectPartitionId:         true,
			expectedTableCaseChecking: true,
			expectedRowsCount:         1,
			expectedTable: ExpectedTable{
				PKFields: utils.NewSet("id"),
				Columns:  justColumns("id", "name", "column_12b241e808ae6c964a5bb9f1c012e63d", "_1test_name", "_2", "column_c16da609b86c01f16a2c609eac4ccb0c", "Test_Name", "Test_Name_DROP_DATABASE_public_SELECT_1_from_DUAL", "Universit_Franais", "_timestamp", "_unnamed", "lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit_sed_do_eiusmod_tempor_incididunt_ut_labore_et_dolore_magna_aliqua_ut_enim_ad_minim_veniam_quis_nostrud_exercitation_ullamco_laboris_nisi_ut_aliquip_ex_ea_commodo_consequat", "column_c41d0d6c9ff6db34c6df393bdd283e19", "column_1cabca1c1b11f3e6e1cd59014c533620", "camelCase", "int", "user", "select", "___ROOT__", "hash"),
			},
			configIds:     []string{BigqueryBulkerTypeId},
			streamOptions: []bulker.StreamOption{WithPrimaryKey("id"), WithMergeRows()},
		},
		{
			name:              "naming_test2",
			tableName:         "Université Français",
			modes:             []bulker.BulkMode{bulker.Batch},
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
