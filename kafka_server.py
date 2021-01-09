import producer_server


def run_kafka_server():
	# Done get the json file path
    input_file = "police-department-calls-for-service.json"

    # Done fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="police-service-calls",
        bootstrap_servers="ç",
        client_id="police-service-calls-stream-producer"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
