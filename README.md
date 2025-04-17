# kafka-sarama

Run kafka local
```
docker compose up --build
```

Visit Kafka UI at
```
localhost:8080
```

Stop kafka
```
docker compose down
```

Run producer
```
go run cmd/producer/main.go 
```

Run consumer
```
go run cmd/consumer/main.go 
```
