#! /bin/sh

purple=\033[1;35m
no_color=\033[0m
yellow=\033[1;33m

dependencies:
	@ docker-compose up -d mariadb.kafkacourse zookeeper.kafkacourse kafka.kafkacourse
	@ echo "\n$(purple)Dependencies are UP$(no_color)\n\n"

restart:
	@ docker-compose down -v
	@ make dependencies

kafka-test:
	@ docker-compose exec kafka.kafkacourse \
		kafka-topics.sh --bootstrap-server localhost:9091 --create --topic test_topic \
		--replication-factor 1 --partitions 1 --if-not-exists

	@ docker-compose exec kafka.kafkacourse \
 		kafka-topics.sh --bootstrap-server localhost:9091 --topic test_topic --describe

	@ echo "\n$(purple)KAFKA TOPIC -- $(yellow)test_topic $(purple)-- WAS CREATED SUCCESSFULLY$(no_color)\n\n"