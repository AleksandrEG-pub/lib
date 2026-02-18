from unittest.mock import patch, MagicMock
from ci_cd.sql_service import MigrateMapping, _construct_sql, _move_from_table_to_table, migrate_data

def test_construct_sql():
    mapping = MigrateMapping(
        source_table="source_tbl",
        target_table="target_tbl",
        source_columns=["col1", "col2"],
        target_columns=["colA", "colB"]
    )
    expected_sql = """
    INSERT INTO target_tbl (colA, colB, loaded_date, record_source)
    SELECT col1, col2,
      current_timestamp as loaded_date,
      'raw-tables' as record_source
    FROM source_tbl
    """
    assert _construct_sql(mapping) == expected_sql


def test_move_from_table_to_table_executes_sql():
    mapping = MigrateMapping(
        source_table="source_tbl",
        target_table="target_tbl",
        source_columns=["col1", "col2"],
        target_columns=["colA", "colB"]
    )
    with patch("database.database_connection.db.cursor") as mock_cursor:
        cursor_instance = MagicMock()
        mock_cursor.return_value.__enter__.return_value = cursor_instance
        _move_from_table_to_table(mapping)
        
        cursor_instance.execute.assert_called_once_with(
            _construct_sql(mapping))


def test_migrate_data_calls_all_mappings():
    with patch("ci_cd.sql_service._move_from_table_to_table") as mock_move:
        migrate_data()
        assert mock_move.call_count == 5
