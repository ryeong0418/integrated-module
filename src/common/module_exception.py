from src.common.enum_module import MessageEnum


class ModuleException(Exception):
    """
    ModuleException class
    """

    def __init__(self, exception_code, logger=None):
        if logger is not None:
            logger.exception(MessageEnum[exception_code].value)

        self.error_code = exception_code
        self.error_msg = MessageEnum[exception_code].value


# class AnalysisClass:
#
#     def main_process(self):
#         from pathlib import Path
#         import os
#
#         home = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent
#         query_folder = f"{home}/export/sql_excel/sql"
#
#         try:
#             sql_name_list = os.listdir(query_folder)
#             sql_split = [i.split(" ") for i in sql_name_list]
#
#             if any(sql for sql in sql_split if len(sql) < 2):
#                 raise ModuleException("E003")
#
#             sql_split_sort = sorted(sql_split, key=lambda x: (int(x[0].split("-")[0]), int(x[0].split("-")[1])))
#
#             print(f"sql_split : {sql_split}")
#
#         except ValueError as ve:
#             raise ModuleException("E002")
#
#
# class Logger:
#     def info(self, v):
#         print(v)
#
#
# if __name__ == "__main__":
#     from resources.config_manager import Config
#     from src.sql.database import DataBase
#     from src.sql.model import ExecuteLogModel
#     from src.common.utils import SystemUtils
#     import time
#
#     ac = AnalysisClass()
#     logger = Logger()
#     logger.info("Start analysis")
#
#     config = Config("local").get_config()
#
#     start_tm = time.time()
#     db = DataBase(config)
#     elm = ExecuteLogModel("test", DateUtils.get_now_timestamp(), "test")
#
#     with db.session_scope() as session:
#         session.add(elm)
#
#     result = "F"
#
#     try:
#         ac.main_process()
#         result = "S"
#         result_code = 'I001'
#         result_msg = MessageEnum[result_code].value
#
#     except ModuleException as me:
#         result_code = me.error_code
#         result_msg = me.error_msg
#         logger.info(f"[ModuleException] ModuleException during analysis cause: {me.error_msg}")
#
#     except Exception as e:
#         logger.info(e)
#         result = "F"
#         result_code = 'E999'
#         result_msg = str(e)[:2000]
#
#     finally:
#         result_dict = SystemUtils.set_update_execute_log(result, start_tm, result_code, result_msg)
#
#         with db.session_scope() as session:
#             session.query(ExecuteLogModel).filter(ExecuteLogModel.seq == f'{elm.seq}').update(result_dict)
#             session.commit()
#
#     logger.info("End analysis")
