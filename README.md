# memc_load

Многопоточный скрипт для парсинга и загрузки статистики в memcached

Работает на python2.7, зависимости в requirements.txt

## запуск и остановка
1. Запуск memcached-инстансов

`make dev-mcstart`

2. Остановка memcached-инстансов

`make dev-mcstop`

3. Запуск загрузчика

`python src/memc_load.py --pattern './data/*gz'`
