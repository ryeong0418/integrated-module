import os

from src.common.enum import ModuleFactoryEnum
from src.common.utils import SystemUtils

class ModuleFactory:

    def __init__(self, logger):
        self.logger = logger

    def get_module_instance(self, process):
        """
        기능 별 instance를 생성하는 함수
        ModuleFactoryEnum에 기능 파라미터 = (모듈명) 으로 등록하고
        모듈명의 Captialized한 class명에 해당하는 instance를 리턴함
        :param process: 동작하려는 기능 파라미터
        :return: 모듈 class instance
        """

        try:
            self.logger.info(f"request process : {ModuleFactoryEnum[process].value}")
            self.logger.info("*" * 79)
            module_name = ModuleFactoryEnum[process].value #initialize
            class_name = SystemUtils.to_camel_case(module_name) #Initialize

            module_path = os.path.dirname(os.path.abspath(__file__))

            target_class = SystemUtils.get_module_class(
                module_name, class_name, module_path
            )

            instance = target_class(self.logger)


        except KeyError as ke:
            self.logger.error(
                f"Process {process} invalid. Please check ModuleFactoryEnum.. Terminated.."
            )
            exit()
        except Exception as e:
            self.logger.exception(
                f"target_class invalid. Please check module and class name in src.. Terminated.."
            )
            exit()
        else:
            return instance
