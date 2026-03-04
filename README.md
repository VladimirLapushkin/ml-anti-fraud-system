# OTUS. Инференс на потоке

Логика такая: один job (kafka-producer) вычитывает данные из бакета и пишет в kafka (топик transactions_in) количество сообщений=(TPS * duration_sec)
второй job берёт из kafka,делает predict на модели mlflow champion и пишет в kafka (топик predictions_out).
Работает run-seconds = DURATION секунд.
т.е. выходит, что его способность предсказать в секунду = количество записанных в kafka (топик predictions_out) / DURATION
Проведено несколько экспериментов по predict 15, 20, 30, 40 секунд
Таким образом определяем порог - в районе 2000 сообщений в секунду (чем меньше времени даём, тем больше % от него идёт на раскачку SPARK)
Скорость Predict обусловлена лёгкой моделью. Но, ка я понимаю, мы сейчас не за сложностью модели гонимся, а за пониманием всей этой балалайки )
в git выложены скрины UI airflow, kafka.


![alt text](screenshot_20260303_184837.jpeg)
![alt text](screenshot_20260303_184900.jpeg)
![alt text](screenshot_20260303_192452.jpeg)
![alt text](screenshot_20260303_192720.jpeg)
![alt text](screenshot_20260303_195801.jpeg)
![alt text](screenshot_20260303_195950.jpeg)
![alt text](screenshot_20260303_200632.jpeg)
![alt text](screenshot_20260303_202058.jpeg)
![alt text](screenshot_20260303_202207.jpeg)
![alt text](screenshot_20260303_202401.jpeg)
![alt text](screenshot_20260303_202441.jpeg)
