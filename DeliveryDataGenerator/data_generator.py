import configparser
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
    data = {'deliveryId': faker.uuid4(),
            'deliveryDate': datetime.now(KST).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
            'userId': faker.simple_profile()['username'],
            'foodCategory': random.choice(['한식', '중식', '양식', '일식', '아시안', '치킨', '버거', '분식']),
            'foodPrice': round(random.randint(5000, 12000) * random.randint(1, 4), -1),
            'paymentMethod': random.choice(['현금', '신용/체크카드', '네이버페이', '카카오페이', '애플페이', '토스페이']),
            'deliveryDistance': round(random.uniform(0.2, 3.0), 1),
            'deliveryDestination': random.choice(seoul_addresses)}

    kakaoApiKey = config['AUTHORIZATION']['KakaoApiKey']
    location = get_location(data['deliveryDestination'], kakaoApiKey)['documents'][0]

    data['destinationLat'] = location['y']
    data['destinationLon'] = location['x']
    data['deliveryCharge'] = round(random.randint(1000, 5000) + (data['deliveryDistance'] * 500), -1)

    return data


def get_location(address, apiKey):
    url = 'https://dapi.kakao.com/v2/local/search/address.json?query=' + address
    headers = {'Authorization': apiKey}
    location = requests.get(url, headers=headers).json()

    return location


def kafka_publish(producer, topic, data):
    producer.produce(
        topic=topic,
        key=data['deliveryId'],
        value=json.dumps(data, ensure_ascii=False),
        on_delivery=delivery_report
    )

    producer.poll(0)


def delivery_report(error, msg):
    if error is not None:
        print(f'Message delivery failed: {error}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('resources/config.ini')

    topic = config['KAFKA']['Topic']
    producer = SerializingProducer({
        'bootstrap.servers': config['KAFKA']['BootstrapServer']
    })

    current_time = datetime.now()

    while (datetime.now() - current_time).seconds < 500:
        try:
            data = generate_delivery_data()
            kafka_publish(producer, topic, data)

            time.sleep(1)
        except Exception as e:
            print(e)

    producer.flush()
