from enum import Enum
import functools
import sqlite3


@functools.total_ordering
class PIPELINE_STATUS(Enum):
    nothing = 0
    downloaded = 1
    finished = 2
    await_approval = 3
    processing = 98
    error = 99

    def __eq__(self, other):
        if other.__class__ is int:
            return self.value == other
        elif other.__class__ is self.__class__:
            return self.value == other.value
        else:
            raise NotImplementedError

    def __lt__(self, other):
        if self.__class__ is not other.__class__:
            raise NotImplementedError
        return self.value < other.value


class FlocsDB:
    def __init__(self, dbname: str, db_table: str):
        self.DATABASE = dbname
        self.TABLE_NAME = db_table

    def get_db_columns(self, obsid: str = None):
        with sqlite3.connect(self.DATABASE) as db:
            db.row_factory = sqlite3.Row
            cursor = db.cursor()
            columns = "target_name,priority,finished,downloaded,sas_id_calibrator1,sas_id_calibrator2,sas_id_calibrator_final,sas_id_target,status_calibrator1,status_calibrator2,status_target,status_vlbi_delay,status_vlbi_dd,status_ddf,status_vlbi_ddf_subtract,status_vlbi_intermediate_img,status_vlbi_facet_subtract,status_vlbi_facet_img"
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

    def set_field_finished(self, name, target):
        query = f"update {self.TABLE_NAME} set finished=1 where target_name=='{name}' and sas_id_target=='{target}'"
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(query)

    def set_final_calibrator(self, name, target, final_cal):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(
                f"update {self.TABLE_NAME} set sas_id_calibrator_final={final_cal} where target_name=='{name}' and sas_id_target=='{target}'"
            )
