import threading
import time

from src.builder_airlines import Airline
from src.builder_airports import Airport
from src.consumer import Run_consumer
from src.producer import Run_producer

airline = Airline()
airport = Airport()
run_producer = Run_producer(server=["localhost:9092"], topic="test_madjid")
run_consumer = Run_consumer(
    server="localhost:9092,", topic="test_madjid", output_folder="./data"
)

# create a producer thread
thread1 = threading.Thread(target=run_producer.run)
thread2 = threading.Thread(target=run_consumer.transform_streaming_data)


thread1.start()
time.sleep(0.5)
thread2.start()

thread1.join()
thread2.join()
