@echo on
cd ./Desktop\co_dev\project
mkdir venv_test
cd venv_test
python -m venv myproject --without-pip
cd myproject
cd Scripts
activate
pause