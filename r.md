## Ссылки
Графана с данными:
https://grafanakns.mos.ru/d/deta170cxxcsga/profile-norvatov?orgId=1&from=1737228054187&to=1737995649699

Influx http://10.126.145.27:8000

## Что
В Influx создана отдельная БД, `requests_data` (создавал Антон Карпов (@Igneumsiderum). Для доступа до Influx нужен подключенный FortiClient.

Наташа скидывала несколько наборов данных:

| Описание                           | Исходник                                | Агрегированные                                            | Скрипт                 | Grafana                                                                                                                               |
|------------------------------------|-----------------------------------------|-----------------------------------------------------------|------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| Логи с июль 2024 по июль 2025 года | архив/res.zip                           | архив/profiles_24-25.rar                                  | data_to_profile.py<br> | https://grafanakns.mos.ru/d/deta170cxxcsga/profile-norvatov?orgId=1&from=1720913109000&to=1753440415000&showCategory=Axis&viewPanel=1 |
| graylog 2024-12-24                 | архив/2412-cleansed.csv                 | архив/2412-cleansed_hourly_aggregated.txt                 | nginx_old_aggregate.py | https://grafanakns.mos.ru/d/deta170cxxcsga/profile-norvatov?orgId=1&from=1735012424923&to=1735060988541&viewPanel=7                   |
| graylog 2025-01-11-2025-01-16      | архив/graylog-2025-01-11-2025-01-16.csv | архив/graylog-2025-01-11-2025-01-16_hourly_aggregated.txt | nginx_old_aggregate.py | https://grafanakns.mos.ru/d/deta170cxxcsga/profile-norvatov?orgId=1&from=1736617105038&to=1737069627043&viewPanel=7                   |
| graylog<br>2025-01-19-2025-01-27   | архив/graylog-2025-01-19-2025-01-27.csv | архив/graylog-2025-01-19-2025-01-27_hourly_aggregated.txt | nginx_old_aggregate.py | https://grafanakns.mos.ru/d/deta170cxxcsga/profile-norvatov?orgId=1&from=1737228054187&to=1737995649699&viewPanel=7                   |
| 2025-07-30                         | nginx_row_data.csv                      | hourly_aggregated.csv                                     | nginx_new_aggregate.py | Не загружал                                                                                                                           |
Вспомогательные скрипты:
`delete_from_influx.py` - удаляет все данные из целевого MEASUREMENT, можно настроить фильтра
`get_from_influx.py` - получить данные из Influx
`influx_client.py` - клиент для подключения к Influx
`load_to_influx.py` - загрузчик данных в Influx (что бы залить данные нужно импортировать соответствующий "обработчик"
`read_profile_summary.py` - скрипт собирает их экселек с профилями по месяцам итоговые данные


## Пояснения
Для заливки новых логов из nginx в Influx понадобятся:
1. Поместить исходники в /data
2. Запустить `load_to_influx.py` 
3. Функция маскирования (применяемые регулярки) берется `из nginx_old_aggregate.py` `mask_url()`
4. `nginx_old_aggregate.py`
	1. генерирует агрегированные по 1 часу, маскированные данные с фильтрацией по source и статику
	2. Статистику Импорта и Экспорта для определения размеров файлов
	3. Пишет в консоль (справочно) перечень Source до и после фильтрации
5. В `nginx_new_aggregate.py` реализовать функцию create_influx_points (можно скопировать из `nginx_old_aggregate.py`, с заменой measurement)