import export_job
import os

# file_list = os.listdir('..\\config\\query') 
# print(file_list)

#export_job.py 모듈 exportJob 클래스 객체 선언
ej = export_job.exportJob()  
#exportJob 클래스 processJob() 메소드 호출
ej.procs_job()