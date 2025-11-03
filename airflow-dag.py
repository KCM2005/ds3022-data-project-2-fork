from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import requests
import boto3

uvaid = "kew6jk"
url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/kew6jk"
submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
platform = "airflow"

def run_pipeline():
    sqs = boto3.client("sqs")

    #Populate queue
    payload = requests.post(url).json()
    print(payload)

    #Wait for messages
  queue_url = payload["sqs_url"]
    while True:
        a = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )["Attributes"]
        total = sum(map(int, a.values()))
        delayed = int(a["ApproximateNumberOfMessagesDelayed"])
        print("Queue:", a)
        if total == 21 and delayed == 0:
            break
        time.sleep(10)

    #Collect messages and assemble phrase
    fragments = []
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            MessageAttributeNames=["All"],
            WaitTimeSeconds=5,
        )
        messages = response.get("Messages", [])
        if not messages:
            break

        for message in messages:
            attrs = message["MessageAttributes"]
            order = int(attrs["order_no"]["StringValue"])
            word = attrs["word"]["StringValue"]
            fragments.append((order, word))
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])

    def get_order(pair):
        order_number, word_fragment = pair  
        return order_number
    
    fragments.sort()
    phrase = " ".join(word_fragment for order_number, word_fragment in fragments)

    print(phrase)

    response = sqs.send_message(
        QueueUrl=submit_url,
        MessageBody="DP2 solution submission",
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": uvaid},
            "phrase": {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform},
        },
    )
    print(f"Response: {response}")

#Implement DAG
with DAG(
    "airflow-dag",
    start_date=datetime(2025, 10, 8),
    schedule_interval=None,
    catchup=False,
) as dag:

    run = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )
