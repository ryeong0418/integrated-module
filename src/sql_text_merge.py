import pandas as pd
import numpy as np
import sqlparse

from src import common_module as cm

from src.analysis_target import InterMaxTarget, SaTarget


class SqlTextMerge(cm.CommonModule):

    def __init__(self, logger):
        self.logger = logger
        self.st = None
        self.imt = None
        self.mgt = None

    def main_process(self):
        self.logger.debug("SqlTextMerge main")

        self.st = SaTarget(self.logger, self.config)
        self.st.init_process()

        # self.st.drop_table_for_sql_text_merge()

        if not self.config['intermax_repo']['use'] and not self.config['maxgauge_repo']['use']:
            self.logger.error(f"intermax_repo or maxgauge_repo use false.. please check config")
            return

        self.imt = InterMaxTarget(self.logger, self.config)
        self.imt.init_process()
        xapm_sql_df = self.imt.get_xapm_sql_text()

        ae_sql_df = self.st.get_ae_db_sql_text()

        merged_df = self._sql_text_merge(xapm_sql_df, ae_sql_df)

        filtered_df = self._merged_df_filter(merged_df)

        # self.imt.insert_ae_sql_text(filtered_df)


        self.logger.debug("SqlTextMerge End")

    def _sql_text_merge(self, xapm_sql_df, ae_sql_df):

        # ae_db_sql_text 테이블은 파티션이 관리되기 때문에 일자별로 중복된 데이터가 있어서 제거 작업 수행
        ae_sql_df = ae_sql_df.drop_duplicates(subset=['sql_uid'], keep='first', inplace=False, ignore_index=False)

        # ae_db_sql_text 테이블 SQL_TEXT 데이터 정제 작업 (공백(' '),탭(\t),엔터(\n),앞엔터(\r))
        ae_sql_df = ae_sql_df.apply(
            lambda x: x.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r", " "], value=["", "", ""], regex=True,
                                inplace=False), axis=1)

        for idx, row in ae_sql_df.iterrows():
            sql_text = row['sql_text']
            # ae_db_sql_text 테이블 SQL_TEXT 데이터 정제 작업 (주석제거,upper)
            ae_sql_df['sql_text'] = np.where(ae_sql_df['sql_text'] == sql_text,
                                         sqlparse.format(sql_text, reindent=False, keyword_case='upper',
                                                         identifier_case='upper', strip_comments=True),
                                         ae_sql_df['sql_text'])

            # 주석 제거 여부 체크를 하기 위한 State_Code 컬럼값 df_ex DataFrame에 추가
        xapm_sql_df = xapm_sql_df.reindex(columns=xapm_sql_df.columns.tolist() + ["state_code"])

        # xapm_sql_text 테이블 SQL_TEXT 데이터 정제 작업 (공백(' '),탭(\t),엔터(\n),앞엔터(\r))
        xapm_sql_df = xapm_sql_df.apply(
            lambda x: x.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r", " "], value=["", "", ""], regex=True,
                                inplace=False), axis=1)

        for idx, row in xapm_sql_df.iterrows():
            # np.where 조건문에 사용하기 위해 기존 SQL_TEXT 값을 sql_text변수에 세팅
            sql_text = row['sql_text']

            # np.where 함수 설명 : (조건문,참일경우 넣는값, 거짓일경우 넣는값)
            # xapm_sql_Text 테이블 SQL_TEXT 데이터 정제 작업 (주석제거,upper)
            xapm_sql_df['sql_text'] = np.where(xapm_sql_df['sql_text'] == sql_text,
                                         sqlparse.format(sql_text, reindent=False, keyword_case='upper',
                                                         identifier_case='upper', strip_comments=True),
                                         xapm_sql_df['sql_text'])

            # 주석 조건을 제외한 정제작업 조건을 맞추기 위해 SQL_TEXT 변수 정제 작업(공백(' '),탭(\t),엔터(\n),앞엔터(\r),upper)
            # sql_text = sql_text.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r", " "], value=["","",""])
            # sql_text = sqlparse.format(sql_text, reindent=False, keyword_case='upper', identifier_case='upper')

            # 주석 제거 전 SQL_TEXT와 제거 후 SQL_TEXT 데이터를 비교하여 State_Code 데이터를 세팅하는 IF문 : 주석제거했으면 1, 않했으면 0으로 세팅
            # df_ex["state_code"] = np.where(df_ex['sql_text'] != sql_text, 1, 0)

        # xapm_sql_text 테이블과 ae_db_sql_text 테이블을 정제된 sql_text 값 기준으로 merge 작업 수행
        df_result = pd.merge(xapm_sql_df, ae_sql_df, how='left', on=['sql_text'])

        return df_result

    def _merged_df_filter(self, merged_df):
        merged_df.rename(columns={'sql_id':'was_sql_id'}, inplace=True)
        merged_df.rename(columns={'sql_uid':'db_sql_uid'}, inplace=True)
        merged_df.drop(["sql_text"], axis=1, inplace=True)
        return merged_df

