

`1.` Создаем новый "vowels"
```shell
docker exec -ti kafka /usr/bin/kafka-topics --create --topic vowels --bootstrap-server localhost:9092
```

`2.` Создаем новый "consonants"
```shell
docker exec -ti kafka /usr/bin/kafka-topics --create --topic consonants --bootstrap-server localhost:9092
```

`3.` Получаем список Топиков которые есть в Kafka брокере, доступном по адресу kafka:9092 и находящемся внутри контейнера Docker с именем kafka:
```shell
docker exec -ti kafka /usr/bin/kafka-topics --list --bootstrap-server kafka:9092
```

`4.` Отправляем сообщение в топик "consonants": 
```shell
docker exec -ti kafka /usr/bin/kafka-console-producer --topic consonants --bootstrap-server kafka:9092
```

`5.` Получить сообщения
```shell
docker exec -ti kafka /usr/bin/kafka-console-consumer --from-beginning --topic consonants --bootstrap-server localhost:9092
```

`6.` Получить сообщения как consumer1
```shell
docker exec -ti kafka /usr/bin/kafka-console-consumer --group consumer1 --topic consonants --bootstrap-server localhost:9092 
```

`7.` Отправляем сообщение в топик "vowels": 
```shell
docker exec -ti kafka /usr/bin/kafka-console-producer --topic vowels --bootstrap-server kafka:9092
```

`8.` Получить сообщения как consumer2
```shell
docker exec -ti kafka /usr/bin/kafka-console-consumer --group consumer2 --topic vowels --bootstrap-server localhost:9092 
```