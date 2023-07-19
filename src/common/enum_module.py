from enum import Enum


class ModuleFactoryEnum(Enum):
    i = "initialize"
    e = "extractor"
    s = "summarizer"
    v = "visualization"
    b = "scheduler"
    m = "sql_text_merge"
    t = "sql_text_template"
    r = "sql_text_similar"


class MessageEnum(Enum):

    # 정상 메세지 I
    I001 = '정상처리완료'

    # 경고 메세지 W
    W001 = '진행중'
    W002 = 'tuning_sql 경로에 tuning sql text가 존재하지 않습니다.'

    # 에러 메세지 E
    E001 = '비정상종료'
    E002 = '추출하려는 sql 파일 형식이 맞지 않습니다. ex)n-n xxxx.txt'
    E003 = '추출하려는 sql 파일 형식이 맞지 않습니다. (공백 없음) ex)n-n xxx.txt'
    E004 = 'intermax_repo or maxgauge_repo use false.. please check config'
    E005 = 'Sql text drain matching invalid target value (choose select or etc)'
    E006 = '실행 parameter를 확인해 주세요. (x_xxxxxxx.bat (start_date) (interval))'
