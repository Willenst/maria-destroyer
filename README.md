# maria-destroyer
Для запуска нужно установить mariaDB и ряд пакетов питона:
```
sudo apt-get update
sudo apt-get install libmariadb3 libmariadb-dev
pip install mariadb
pip install pytest
pip install pytest-ordering
pip install pytest-benchmark
```

Зайти в базу:
```
sudo mariadb
```

Внутри базы:
```
CREATE USER admin@localhost IDENTIFIED BY 'password';
CREATE DATABASE test123;
```

В целом после этого можно запускать код.
```
py.test -s -v test_1_1.py --benchmark-group-by=func --benchmark-sort=Name --benchmark-min-rounds=10
```
