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
    W001 = '경고메세지'

    # 에러 메세지 E
    E001 = '비정상종료'
