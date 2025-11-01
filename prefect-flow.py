import requests
import boto3
import time

#Task 1: Populate SQS Queue
url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/kew6jk"

payload = requests.post(url).json()

print(payload)


#Task 2: Monitor queue then collect messages
queue_url = payload["sqs_url"] 
sqs = boto3.client("sqs")
fragments = []

while True:
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )["Attributes"] 
      
    visible = int(attrs["ApproximateNumberOfMessages"])
    not_visible = int(attrs["ApproximateNumberOfMessagesNotVisible"])
    delayed = int(attrs["ApproximateNumberOfMessagesDelayed"])
    total = visible + not_visible + delayed

    print(f"Queue status: {attrs} (Total = {total})")
    
    #Exit loop when all 21 messages exist and none are delayed
    if total == 21 and delayed == 0:
        print("All messages ready for retrieval")
        break

    time.sleep(10)

#Retrieve, parse, and delete all messages
while True:
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            MessageAttributeNames=["All"],
            WaitTimeSeconds=5
        )
    except Exception as e:
        print("Error receiving messages:", e)
        time.sleep(5)
        continue

    messages = response.get("Messages", [])
    if not messages:
        print("No more messages — collection complete.")
        break

    for msg in messages:
        attrs = msg.get("MessageAttributes", {})
        if not attrs:
            print("Skipping message with no attributes.")
            continue

        try:
            order_no = int(attrs["order_no"]["StringValue"])
            word = attrs["word"]["StringValue"]

        except KeyError:
            print("Missing expected message attributes, skipping message.")
            continue

        receipt_handle = msg["ReceiptHandle"]
        fragments.append((order_no, word))

        # Delete message after storing data
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        print(f"Deleted message {order_no}: {word}")

    time.sleep(1)

#Sort and assemble full phrase
def get_order(pair):
    order_number, word_fragment = pair  
    return order_number

fragments.sort(key=get_order)
phrase = " ".join(word_fragment for order_number, word_fragment in fragments)

print(phrase)

#Task 3: Reassemble messages and submit via SQS
submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
uvaid = "kew6jk"
platform = "prefect"

def send_solution(uvaid, phrase, platform):
    try:
        message = "DP2 solution submission"
        response = sqs.send_message(
            QueueUrl=submit_url,
            MessageBody=message,
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        print(f"Response: {response}")

        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print("Solution submitted successfully. HTTP 200 received.")
        else:
            print("Submission returned unexpected status.")

    except Exception as e:
        print("Error submitting solution:", e)

send_solution(uvaid, phrase, platform)  
