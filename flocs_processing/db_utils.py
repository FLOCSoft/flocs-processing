import sqlite3

from flocs_processing.flocs_processing import FIELD_STATUS, PIPELINE_STATUS


class FlocsDB:
    def __init__(self, dbname: str, db_table: str):
        self.DATABASE = dbname
        self.TABLE_NAME = db_table

    def get_db_columns(self, obsid: str = ""):
        with sqlite3.connect(self.DATABASE) as db:
            db.row_factory = sqlite3.Row
            cursor = db.cursor()
            columns = "*"
            if obsid:
                field = cursor.execute(
                    f"select {columns} from {self.TABLE_NAME} where sas_id_target=='{obsid}' and finished==0 order by priority desc"
                ).fetchall()
            else:
                field = cursor.execute(
                    f"select {columns} from {self.TABLE_NAME} where finished==0 order by priority desc"
                ).fetchall()
            print(field)
        return field

    def reset_field(self, name, target):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(
                f"update {self.TABLE_NAME} set status={FIELD_STATUS.nothing.value} where target_name=='{name}' and sas_id_target=='{target}'"
            )

    def set_field_downloading(self, name, target):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(
                f"update {self.TABLE_NAME} set status={FIELD_STATUS.downloading.value} where target_name=='{name}' and sas_id_target=='{target}'"
            )

    def set_field_processing(self, name, target):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(
                f"update {self.TABLE_NAME} set status={FIELD_STATUS.processing.value} where target_name=='{name}' and sas_id_target=='{target}'"
            )

    def set_field_finished(self, name, target):
        query = f"update {self.TABLE_NAME} set status={FIELD_STATUS.processing.value} where target_name=='{name}' and sas_id_target=='{target}'"
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(query)

    def set_status_nothing(self, name, identifier, target):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(
                f"update {self.TABLE_NAME} set status_{identifier}={PIPELINE_STATUS.nothing.value} where target_name=='{name}' and sas_id_target=='{target}'"
            )

    def set_status_failed(self, name, identifier, target):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(
                f"update {self.TABLE_NAME} set status_{identifier}={PIPELINE_STATUS.error.value} where target_name=='{name}' and sas_id_target=='{target}'"
            )

    def set_status_processing(self, name, identifier, target):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(
                f"update {self.TABLE_NAME} set status_{identifier}={PIPELINE_STATUS.processing.value} where target_name=='{name}' and sas_id_target=='{target}'"
            )

    def set_status_await_approval(self, name, identifier, target):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(
                f"update {self.TABLE_NAME} set status_{identifier}={PIPELINE_STATUS.await_approval.value} where target_name=='{name}' and sas_id_target=='{target}'"
            )

    def set_status_finished(self, name, identifier, target):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(
                f"update {self.TABLE_NAME} set status_{identifier}={PIPELINE_STATUS.finished.value} where target_name=='{name}' and sas_id_target=='{target}'"
            )

    def set_status_downloaded(self, name, target):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(
                f"update {self.TABLE_NAME} set downloaded=1 where target_name=='{name}' and sas_id_target=='{target}'"
            )

    def set_final_calibrator(self, name, target, final_cal):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(
                f"update {self.TABLE_NAME} set sas_id_calibrator_final={final_cal} where target_name=='{name}' and sas_id_target=='{target}'"
            )
