# -*- Coding = UTF-8 -*-
# Author: Nico
# File: SellHouse.py
# Software: PyCharm
# Time: 2024/1/26 12:03

import time
import parsel
import random
import sqlite3
import requests
from flask import Flask, render_template, request
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
scheduler = BackgroundScheduler()


def get_house_data(offset, limit):
    conn = sqlite3.connect('Data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT * FROM house ORDER BY CAST(totalprice AS INTEGER) LIMIT ? OFFSET ?', (limit, offset))
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


def count_total_houses():
    conn = sqlite3.connect('Data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(DISTINCT href) FROM house')
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count


def count_houses_under_300():
    conn = sqlite3.connect('Data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(DISTINCT href) FROM house WHERE CAST(totalprice AS INTEGER) <= 300')
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count


@app.route('/')
def house():
    page = request.args.get('page', 1, type=int)
    items_per_page = 30
    offset = (page - 1) * items_per_page
    house_data = get_house_data(offset, items_per_page)
    total_houses = count_total_houses()
    total_pages = (total_houses + items_per_page - 1) // items_per_page
    houses_under_300 = count_houses_under_300()
    return render_template('house.html', house0=house_data, house1=total_houses, house2=houses_under_300, total_pages=total_pages, current_page=page)


def run_spider_job():
    conn = sqlite3.connect('Data.db')
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS house')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS house (
            name TEXT,
            type TEXT,
            area TEXT,
            face TEXT,
            floor TEXT,
            unitprice TEXT,
            totalprice TEXT,
            href TEXT
        )
    ''')
    conn.commit()

    for page in range(1, 101):
        time.sleep(random.randint(1, 3))
        url = f'https://sh.lianjia.com/ershoufang/pudong/pg{page}/'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36'
        }
        response = requests.get(url=url, headers=headers)
        selector = parsel.Selector(response.text)
        divs = selector.css('div.info.clear')
        for div in divs:
            area_list = div.css('.positionInfo a::text').getall()
            name = '-'.join(area_list)
            info = div.css('.houseInfo::text').get().split('|')
            type = info[0]
            area = info[1].replace('平米', '㎡')
            face = info[2]
            floor = info[4]
            unitprice = div.css('.unitPrice span::text').get().replace('单价', '').replace('平', '㎡')
            totalprice = div.css('.totalPrice span::text').get()
            href = div.css('.title a::attr(href)').get()
            cursor.execute('''
                INSERT INTO house (name, type, area, face, floor, unitprice, totalprice, href)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (name, type, area, face, floor, unitprice, totalprice, href))
    conn.commit()
    conn.close()


if __name__ == '__main__':
    scheduler.add_job(run_spider_job, trigger=CronTrigger(hour=00, minute=00))
    scheduler.start()
    app.run(threaded=True, host='0.0.0.0', port=5000)
