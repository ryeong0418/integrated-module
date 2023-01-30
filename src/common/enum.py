from enum import Enum


class ModuleFactoryEnum(Enum):
    i = "initialize"
    e = "extractor"
    s = "summarizer"
    v = "visualization"
    b = "scheduler"
    m = "sql_text_merge"
