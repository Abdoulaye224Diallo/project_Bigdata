## Lancer le container
- docker-compose up -d

## Lancer les producers
- python .\producer\stationnement_consumer.py
- python .\producer\sncf_consumer.py

## Ensuite lancer le fichier pyhon pour consommer les données envoyer par le producer et uploder dans le datalake qui est notre Minio
- python .\consumer\stationnement_consumer.py
- python .\consumer\sncf_consumer.py

## Lancer le docker Kafka et Zookeepers sur le topic
-  docker exec -it kafka_broker kafka-topics --bootstrap-server localhost:9092 --list
stationnement-data 
- docker exec -it kafka_broker kafka-console-consumer --bootstrap-server localhost:9092 --topic sncf-data --from-beginning


# lancer le container Postegres 
- docker-compse up -d
# Lancer la postgres
- docker exec -it datamart_postgres psql -U postgres -d datamart
- \dt










