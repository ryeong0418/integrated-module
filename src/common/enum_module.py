from enum import Enum


class ModuleFactoryEnum(Enum):
    i = "initialize"
    e = "extractor"
    s = "summarizer"
    v = "visualization"
    b = "scheduler"
    m = "sql_text_merge"


class MessageEnum(Enum):

    # 정상 메세지 I
    I001 = '정상처리완료'

    # 경고 메세지 W
    W001 = '진행중'

    # 에러 메세지 E
    E001 = '비정상종료'
    E002 = '추출하려는 sql 파일 형식이 맞지 않습니다. ex)n-n xxxx.txt'
    E003 = '추출하려는 sql 파일 형식이 맞지 않습니다. (공백 없음) ex)n-n xxx.txt'
