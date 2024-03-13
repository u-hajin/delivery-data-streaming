import json
import random
import time

import requests

from confluent_kafka import SerializingProducer
from datetime import datetime
from faker import Faker
from address import seoul_addresses

faker = Faker()


def generate_delivery_data():
    return {
        'deliveryId': faker.uuid4(),
        'deliveryDate': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
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


if __name__ == '__main__':
    delivery = generate_delivery_data()

    location = get_location(delivery['deliveryDestination'])['documents'][0]

    delivery['lat'] = location['y']
    delivery['lon'] = location['x']
    delivery['deliveryCharge'] = round(random.randint(1000, 5000) + (delivery['deliveryDistance'] * 500), -1)
