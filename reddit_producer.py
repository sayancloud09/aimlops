import praw
import time
from kafka import KafkaProducer
import json

# Initialize Reddit API with authentication credentials
reddit = praw.Reddit(
    client_id="PSssY2fCBwf8SLSykLYjUg",  # Reddit API client ID
    client_secret="E9A3tMBxOpLrHC830enE5wfkhTazUQ",  # Reddit API secret key
    user_agent="sentibot"  # User agent to identify the bot
)

# Initialize Kafka Producer to send data to Kafka topic
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize messages as JSON
)

# Kafka topic to send the data
TOPIC_NAME = "wallstreetbets-stream"
#TOPIC_NAME = "IndiaTech-stream"

# Sets to store processed post and comment IDs to avoid duplicates
processed_posts = set()  # Store processed post IDs
processed_comments = set()  # Store processed comment IDs

# Infinite loop to continuously fetch Reddit data
while True:
    print("Fetching new Reddit posts and comments...")

    # Fetch latest 5 posts from r/wallstreetbets
    for submission in reddit.subreddit("wallstreetbets").new(limit=10):
    #for submission in reddit.subreddit("IndiaTech").new(limit=5):
        if submission.id not in processed_posts:  # Check if the post has already been processed
            post_data = {
                "type": "post",
                "id": submission.id,
                "title": submission.title,
                "selftext": submission.selftext,
                "url": submission.url,
                "created_utc": submission.created_utc
            }

            # Log the fetched post
            print(f"Fetched New Post: {submission.title}")

            # Send the post data to Kafka topic
            producer.send(TOPIC_NAME, value=post_data)

            # Mark the post as processed to prevent duplicates
            processed_posts.add(submission.id)

            # Fetch comments for the post
            submission.comments.replace_more(limit=0)  # Ensure all comments are loaded
            for comment in submission.comments.list():
                if comment.id not in processed_comments:  # Check if the comment is new
                    comment_data = {
                        "type": "comment",
                        "id": comment.id,
                        "parent_id": comment.parent_id,
                        "body": comment.body,
                        "created_utc": comment.created_utc
                    }

                    # Log the fetched comment
                    print(f"Fetched New Comment: {comment.body[:50]}")  # Show first 50 chars

                    # Send the comment data to Kafka topic
                    producer.send(TOPIC_NAME, value=comment_data)

                    # Mark the comment as processed to prevent duplicates
                    processed_comments.add(comment.id)

    # Ensure all messages are sent to Kafka before waiting
    producer.flush()

    # Wait for 1 minute before checking for new posts/comments
    print("Waiting for new posts and comments...")
    time.sleep(60)
