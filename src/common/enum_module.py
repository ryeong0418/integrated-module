from enum import Enum


class ModuleFactoryEnum(Enum):
    """
    ModuleFactoryEnum class
    """

    i = "initialize"
    e = "extractor"
    s = "summarizer"
    v = "visualization"
    b = "scheduler"
    m = "sql_text_merge"
    t = "sql_text_template"
    r = "sql_text_similar"
    c = "metric_performance_report"
    d = "dynamic_sql_search"
    p = "dynamic_sql_search"


class MessageEnum(Enum):
    """
    MessageEnum class
    """

    # 정상 메세지 I
    I001 = "정상처리완료"

    # 경고 메세지 W
    W001 = "진행중"
    W002 = "tuning_sql 경로에 tuning sql text가 존재하지 않습니다."
    W003 = "Dynamic sql이 존재 하지 않습니다."
    W004 = "분석된 대상이 없습니다"

    # 에러 메세지 E
    E001 = "비정상종료"
    E002 = "추출하려는 sql 파일 형식이 맞지 않습니다. ex)n-n xxxx.txt"
    E003 = "추출하려는 sql 파일 형식이 맞지 않습니다. (공백 없음) ex)n-n xxx.txt"
    E004 = "intermax_repo or maxgauge_repo use false.. please check config"
    E005 = "Sql text drain matching invalid target value (choose select or etc)"
    E006 = "실행 parameter를 확인해 주세요. (x_xxxxxxx.bat (start_date) (interval))"
    E007 = "SqlTextMerge 기능은 was, db 모두 데이터가 필요 합니다. resources/config/config.json에 정확히 정보를 입력해주세요."
    E008 = "지원하지 않는 DB type 입니다. invalid collector_db_type (check config.json collector_db_type key)"
    E009 = "oracle 접속 service_name / sid 정보가 없습니다."
    E010 = "다이나믹 SQL 분석을 위한 사전 처리된 데이터가 없습니다."
    E011 = "DynamicSqlParse 중 오류가 발생했습니다. master.log를 확인해주세요."
