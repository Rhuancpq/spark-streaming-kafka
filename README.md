# Projeto de Pesquisa Spark-Kafka

## Integrantes
16/0119316 - Ezequiel de Oliveira

18/0033646 - João Luis Baraky

18/0054848 - Rhuan Carlos

## Requisitos
 - ***Docker*** 
 - ***docker-compose***

## Execução
Para executar não é necessário configuração, basta subir primeiramente o Kafka, Zookeper e Hadoop com o comando:

```shell
$ docker-compose up --build hadoop zookeeper kafka
```

E em seguida, subir a aplicação:

```shell
$ docker-compose up --build
```

