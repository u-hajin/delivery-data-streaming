import json
import random
import time

import requests

from confluent_kafka import SerializingProducer
from datetime import datetime, timedelta, timezone
from faker import Faker
from address import seoul_addresses

faker = Faker()
KST = timezone(timedelta(hours=9))


def generate_delivery_data():
    return {
        'deliveryId': faker.uuid4(),
        'deliveryDate': datetime.now(KST).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        'userId': faker.simple_profile()['username'],
        'foodCategory': random.choice(['한식', '중식', '양식', '일식', '아시안', '치킨', '버거', '분식']),
        'foodPrice': round(random.randint(5000, 12000) * random.randint(1, 4), -1),
        'paymentMethod': random.choice(['현금', '신용/체크카드', '네이버페이', '카카오페이', '애플페이', '토스페이']),
        'deliveryDistance': round(random.uniform(0.2, 3.0), 1),
        'deliveryDestination': random.choice(seoul_addresses),
    }


def get_location(address):
    url = 'https://dapi.kakao.com/v2/local/search/address.json?query=' + address
    headers = {'Authorization': 'KakaoAK <APIKEY>'}
    location = requests.get(url, headers=headers).json()

    return location


def delivery_report(error, msg):
    if error is not None:
        print(f'Message delivery failed: {error}')
    else:
        print(f'Message delivered to {msg.topic} [{msg.partition}]')


if __name__ == '__main__':
    topic = 'delivery_information'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092,localhost:9093'
    })

    current_time = datetime.now()

    while (datetime.now() - current_time).seconds < 200:
        try:
            delivery = generate_delivery_data()
            location = get_location(delivery['deliveryDestination'])['documents'][0]

            delivery['destinationLat'] = location['y']
            delivery['destinationLon'] = location['x']
            delivery['deliveryCharge'] = round(random.randint(1000, 5000) + (delivery['deliveryDistance'] * 500), -1)

            producer.produce(
                topic=topic,
                key=delivery['deliveryId'],
                value=json.dumps(delivery, ensure_ascii=False),
                on_delivery=delivery_report
            )

            producer.poll(0)

            time.sleep(2)
        except Exception as e:
            print(e)
