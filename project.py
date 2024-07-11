import mariadb
import sys
import pytest
import random
import time

'''
letters - входной набор данных для генерации слов, чем больше букв тем случайнее слово
dimension - количество строчек в базе данных
minlen - минимальная длинна слова
maxlen - максимальная длинна слова
step - шаг увеличения длинны слова
searchsize - размерность случайного слова применяемого для теста производительности
run_qerry - количество выполняемых поисков в рамках одного теста
lenmultiplayer - модификатор длинны слова, увеличивает число букв в слове.
skip_fucntional - выключение тестов на функционал
skip_performance - выключение тестов на производительность


в зависимости от входных данных результат может быть иным, особенно на небольших размерностях, 
введенные на текущий момент данные были использованы в отчете. На значениях размерности выше 100.000 и количестве поисковых запросов выше 1000
начинает проседать скорость работы в экспоненциальном объеме.

запуск можно производить через команду:

py.test -s -v test_1_1.py --benchmark-group-by=func --benchmark-sort=Name --benchmark-min-rounds=10
'''
letters = 'abcde'
dimension = 5000
minlen = 4
maxlen = 9
step=1
searchsize = 4
run_qerry=100
lenmultiplayer=5
skip_fucntional=False
skip_performance=False

#для полного функционала индексов нам нужны строчки идентичного характера, сгенерируем их из следующего набора букв
#также для наглядности будем постепенно повышать энтропию набора увеличивая длинну строки
test_data_sets=[]
randlines=[]
for k in range(minlen,maxlen+1,step):
    randlines=[]
    for i in range(dimension):
        randstr = ''
        for j in range(k):
            randstr = randstr + lenmultiplayer*random.choice(letters)
        randlines.append(randstr)
    template = random.choice(randlines)
    count=0  
    for line in randlines:
        if template in line:
            count+=1
    test_data_sets.append([count]+[template]+randlines)   




#задаем фикстуры
@pytest.fixture(scope="module", params=[run_qerry])
def param_test(request):
    return request.param

#Задание базовой фикстуры в рамках всех тестов - на подключение к БД
@pytest.fixture(scope="module") 
def db_connection(request):
    try:
        conn_params = {
            'user' : "admin",
            'password' : "password",
            'host' : "127.0.0.1",
            'port' : 3306,
            'database' : "test123"
        }
        connection = mariadb.connect(**conn_params)
        cursor = connection.cursor()
        
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
    def resource_teardown(): #Завершающий сегмент фикстуры на отключение от бд
        print("\ndisconnect")
        cursor.close()
    request.addfinalizer(resource_teardown)
    yield connection

#фикстура на заполнения БД данными для тестирования
@pytest.fixture(params=test_data_sets, scope="function")
def setup_data(db_connection, request):
    cursor = db_connection.cursor()
    
    #очистка перед каждым тестом
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("CREATE TABLE test (info1 VARCHAR (400) NOT NULL, info2 VARCHAR (400) NOT NULL, info3 VARCHAR (400) NOT NULL, info4 VARCHAR (400) NOT NULL)")

    #вставка данных и заполнение других столбцов хламом
    for data in request.param[2:]:
        rand1=''.join(random.choices('qwertyuiopadsffghjklzxcvbnm1234567890', k=8))
        rand2=''.join(random.choices('qwertyuiopadsffghjklzxcvbnm1234567890', k=8))
        rand3=''.join(random.choices('qwertyuiopadsffghjklzxcvbnm1234567890', k=8))
        cursor.execute("INSERT INTO test(info1, info2, info3, info4) VALUES (%s, %s, %s, %s)", (data, rand1, rand2, rand3))
        l=len(data)
    db_connection.commit()
    return request.param[0],request.param[1], l

#фикстура на создание индекса, он 
# будет создаваться каждый раз при запуске теста
@pytest.fixture(scope="function")
def create_indexes(db_connection, request):
    cursor = db_connection.cursor()
    cursor.execute("DROP INDEX IF EXISTS info1_index ON test")
    # Создание индекса на колонку str
    cursor.execute("CREATE INDEX info1_index ON test (info1)")
    db_connection.commit()
    time.sleep(3) #даем базе чуток обдумать созданные индексы
    def resource_teardown(): #Завершающий сегмент фикстуры на отключение от бд
        cursor.execute("DROP INDEX info1_index ON test")
        db_connection.commit()
    request.addfinalizer(resource_teardown)

#далее идут уже сами тесты

#функциональный тест на поиск без индекса
@pytest.mark.run(order=1)
@pytest.mark.skipif(skip_fucntional==True, reason="disabled")
def test_without_index_functional(db_connection, setup_data):
    cursor = db_connection.cursor()

    # Выполнение запроса без индекса
    cursor.execute("SELECT * FROM test WHERE info1 LIKE (%s)", ('%'+setup_data[1]+'%',))
    results_without_index = cursor.fetchall()
    res=len(results_without_index)
    print('\nfound '+str(res)+' lines')
    assert res == setup_data[0]  # Ожидаем строки, соответствующие шаблону

#функциональный тест на поиск с индексом
@pytest.mark.run(order=2)
@pytest.mark.skipif(skip_fucntional==True, reason="disabled")
def test_with_index_functional(db_connection, setup_data, create_indexes):
    cursor = db_connection.cursor()

    # Выполнение запроса с индексом
    cursor.execute("SELECT * FROM test WHERE info1 LIKE (%s)", ('%'+setup_data[1]+'%',))
    results_with_index = cursor.fetchall()
    res=len(results_with_index)
    print('\nfound '+str(res)+' lines')

    assert res == setup_data[0]  # Ожидаем строки, соответствующие шаблону

@pytest.mark.run(order=3)
#перформанс тест на поиск без индекса
@pytest.mark.skipif(skip_performance==True, reason="disabled")
def test_without_index_performance(db_connection, setup_data, param_test, benchmark):
    @benchmark
    def test1():
        cursor = db_connection.cursor()

        # Выполнение запроса без индекса
        for i in range (param_test):
            cursor.execute("SELECT * FROM test WHERE info1 LIKE (%s)", ('%'+searchsize*random.choice(letters)+'%',))
            cursor.fetchall()
        cursor.close()

@pytest.mark.run(order=4)
#перформанс тест на поиск с индексом
@pytest.mark.skipif(skip_performance==True, reason="disabled")
def test_with_index_performance(db_connection, setup_data, create_indexes, param_test, benchmark):
    @benchmark
    def test1():
        cursor = db_connection.cursor()
        # Выполнение запроса с индексом
        for i in range (param_test):
            cursor.execute("SELECT * FROM test FORCE INDEX (info1_index) WHERE info1 LIKE (%s)", ('%'+searchsize*random.choice(letters)+'%',))
            cursor.fetchall()
        cursor.close()
