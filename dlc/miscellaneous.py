from storage import db_cursor

TABLE_NAME = "testing_table"
HISTORY_TABLE = "history_testing_table"

def save_case_history(case_id):
    with db_cursor() as cur:
        cur.execute(
            f"""
                INSERT INTO {HISTORY_TABLE}
                SELECT * FROM {TABLE_NAME} WHERE case_id = ?;
            """, (case_id,)
        )
