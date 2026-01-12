# OTUS. Практика. Конвейер подготовки данных
в задачах много выполненнных с ошибкой задач - это отладка
## скрин задач- граф
![alt text](task_graph.jpeg)
## скрин задач- граф 2
![alt text](task_graph-2.jpeg)
## скрин - гант
![alt text](task_gantt.jpeg)
## скрин - бакет с очищенными данными
![alt text](clean_data.jpeg)
## скрин - бакет с очищенными данными в формате parquet
![alt text](clean_data_parquet.jpeg)
## скрин - xcom
![alt text](xcom.jpeg)
## скрин - variables - видно выбор файлов с индексом из списка
![alt text](variables.jpeg)

## файлы из бакета источника по одному за проход (выполнение dag) очищаются и размещаются в целевом баете, там же размещаются логи.
## добавлен файл make_s3cmd.ini.cmd - генерация файла s3cmd.ini и размещение в директории (для windows)
после выполнения terraform apply следует запустить make_s3cmd.ini.cmd для возможности затем выполенния make (загрузка src и dag)
## код terraform адаптирован для windows (powershell)
