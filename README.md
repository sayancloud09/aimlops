#Install Python Dependencies
pip install -r requirements.txt

#Start Docker Services
#Start Zookeeper and Kafka using Docker Compose: docker-compose up -d
#Check if services are running: docker ps

#Run the Consumer
In Terminal 1:
python reddit_consumer.py

#Run the Producer
In Terminal 2:
python reddit_producer.py


#Run the streamlit app 
In Terminal 3:
python3 -m streamlit run app.py --server.port 8501 --server.address 0.0.0.0
