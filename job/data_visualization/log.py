# logging 모듈 import install 하지 않아도 된다. 
#해당 log.py를 사용하고자 하는 .py와 동일한 디렉토리에 놓고 import log.py를 하면 get_logger() 호출을 통해 사용할 수 있다.
from logging import handlers
import logging

#if __name__ == '__main__':
def get_logger(name=None):
    
    # log settings : formatter 지정하여 log head를 구성해줍니다. 
    ## asctime - 시간정보
    ## levelname - logging level
    ## funcName - log가 기록된 함수
    ## lineno - log가 기록된 line
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s")    
 
    # handler settings : 일별 로그 파일을 생성하고 기록하기 위한 handler를 세탕합니다.
    ## filename - 로그 파일명
    ## when/interval - 저장 주기 설정 시 midnight -> 매일밤 자정에 새 로그 파일이 만들어진다(interval 은 atTime을 지정하지 않으면 자정에 수행)
    ## backupCount - 최대 백업할 로그 파일의 개수( 0으로 세팅시 무한 생성 )
    ## suffix - 파일이 만들어지는 형식 기준 log_debug.log뒤에 suffix(%Y%m%d)가 추가된다
    timedfilehandler = logging.handlers.TimedRotatingFileHandler(filename="..\\..\\..\\log\\log_debug.log", when='midnight', interval=1, backupCount=0, encoding='utf-8')
    timedfilehandler.setFormatter(formatter)
    timedfilehandler.suffix = "%Y%m%d"        
    
    #logger set : 사용하기 위해 log instance를 만들고 log level 설정과 핸들러정보 추가한다.
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(timedfilehandler)	 
    
    #console에 log 내용을 출력하기 위한 로직
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(formatter)
    logger.addHandler(console)
   
    # 설정된 log setting을 반환합니다.
    return logger