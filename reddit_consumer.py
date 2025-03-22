from kafka import KafkaConsumer
import json
import pandas as pd

# Initialize Kafka Consumer to consume messages from the 'wallstreetbets-stream' topic
consumer = KafkaConsumer(
    'wallstreetbets-stream',  # Kafka topic to listen to
   # "IndiaTech-stream",
    bootstrap_servers='localhost:9092',  # Kafka broker address
    auto_offset_reset='earliest',  # Start consuming from the earliest message available
    enable_auto_commit=True,  # Automatically commit message offsets
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # Deserialize JSON messages
)

# Dictionary to store post titles with their IDs for reference
posts = {}

# Lists to temporarily store data before writing to files in batches
posts_data = []
comments_data = []

# File paths for storing posts and comments data
posts_file = 'posts.csv'
comments_file = 'comments.csv'

# Define batch size for writing to CSV files (avoids writing for every message)
batch_size = 10  

while True:  # Infinite loop to keep consuming messages
    for message in consumer:  # Iterate over messages received from Kafka
        data = message.value  # Extract message content

        # If the received data is a post, store its details
        if data["type"] == "post":
            posts[data['id']] = data['title']  # Store post title with its ID
            posts_data.append([
                data['id'],  # Post ID
                data['title'],  # Post title
                data['selftext'],  # Post content
                data['created_utc']  # Timestamp
            ])
            print(f"Saved Post: {data['title']}")  # Log the saved post

        # If the received data is a comment, store its details
        elif data["type"] == "comment":
            parent_id = data['parent_id'].replace('t3_', '')  # Extract post ID from parent_id
            post_title = posts.get(parent_id, "Unknown Post")  # Get the post title for reference
            comments_data.append([
                data['id'],  # Comment ID
                post_title,  # Associated post title
                data['body'],  # Comment content
                data['created_utc']  # Timestamp
            ])
            print(f"Saved Comment: {data['body'][:50]}")  # Print first 50 characters of the comment

        # Save post data to CSV when batch size is reached
        if len(posts_data) >= batch_size:
            df_posts = pd.DataFrame(posts_data, columns=["post_id", "title", "selftext", "created_utc"])
            df_posts.to_csv(posts_file, mode='a', header=False, index=False)  # Append to CSV
            posts_data = []  # Clear buffer after saving

        # Save comment data to CSV when batch size is reached
        if len(comments_data) >= batch_size:
            df_comments = pd.DataFrame(comments_data, columns=["comment_id", "post_title", "comment", "created_utc"])
            df_comments.to_csv(comments_file, mode='a', header=False, index=False)  # Append to CSV
            comments_data = []  # Clear buffer after saving
