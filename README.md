# AE Group WAS&DB Data Analysis Using Python

개발 환경 구성하기 
1. python 설치 하기 
2. Visual Studio Code 설치하기
3. git-scm 설치하기 
* https://oequalsto0.tistory.com/entry/python-%EA%B0%9C%EB%B0%9C%ED%99%98%EA%B2%BD-%EA%B5%AC%EC%B6%95%ED%95%98%EA%B8%B02-github

* python 필수 모듈 Install 리스트
  - pip install psycopg2
  - pip install pandas
  - pip install sqlalchemy
  - pip install pyarrow 
  - pip install xlwt
  - pip install openpyxl
  - pip install sqlparse
  
* 원격저장소 정보 가져오기
 1) 원격 저장소에서 코드 가져오기 
    -> git clone https://github.com/PJH0310/ae.git
    이후 자신이 작업할 Branch를 만들고 해당 branch에서 하단 내용을 수행하여 main(원격저장소)에 있는 로직을 로컬저장소에 저장한다.
    (branch 작업 코드는 제일 하단 내용 참고)
 2) 원격저장소 main 코드를 내 branch 로컬저장소로 가져오기 
    -> 로컬 브랜치로 접속된 상태에서 git pull origin main 

*반영하기 전 초기 설정 
   1) git config --global user.email "fabulos@naver.com"
   2) git config --global user.name "shpark75"

* 반영하는 방법 (로컬저장소)
  1) 자신이 작업하는 branch 에서 -> git add . 
  2) -> git commit -m "반영내용" 수행 (작업을 수행하는 디렉토리 하위만 Commit 됨)
  3) -> git push origin 브랜치명 수행 

* 반영하는 방법 (원격저장소)
  1) 자신이 작업하는 branch 에서 
     -> git add . 
     -> git commit -m "반영내용"
     -> git push origin shpark

  2) main brach 로 접속 후 merge 작업 수행 
     -> git checkout main 
     -> git merge (브랜치 명)
     -> git push

     예시 
         * My Local branch에 반영

         git add .
         git commit -m "반영내용"
         git push origin shpark

         * Main branch에 반영

         git checkout main
         git merge shpark
         git push

* 내가 작업한 로직이 로컬저장소, 원격저장소에서 동일한지 확인하는 방법
   1) git status 를 통해 빨간색으로 표기된 부분은 일치하지 않는다

* Branch 사용방법 (자신이 작업할 branch 를 만들어 작업하고 로컬저장소에 저장된 데이터를 원격 저장소로 저장하기 위해서는 병합하기 명령어를 사용한다.)
  1) Branch 생성 -> git branch [name]
  2) Branch 목록 보기 -> git branch
  3) 지정한 Branch 삭제 -> git branch -d [name]
  4) 로컬저장소의 Branch를 전환 -> git checkout [name]

  참고 URL : https://ifuwanna.tistory.com/283
   