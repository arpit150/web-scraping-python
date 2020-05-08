import psycopg2
from lxml import html
import requests as r
import sys

page = 4953//25
start = 749//25


conn = psycopg2.connect(user="postgres",
                               password="arpit",
                               host="127.0.0.1",
                               port="5432", 
                               database="web_watcher")

cur = conn.cursor()
# print("connection created")
# cur.execute("DROP TABLE IF EXISTS  master.holidayiq cascade ")
# print("table dropped")

# cur.execute("create table master.holidayiq(id serial NOT NULL,name varchar(255) NOT NULL DEFAULT NULL::character varying,address varchar(255) NOT NULL DEFAULT NULL::character varying,cost varchar(255) NOT NULL DEFAULT NULL::character varying,rating varchar(255) NOT NULL DEFAULT NULL::character varying,no_of_reviews varchar(255) NOT NULL DEFAULT NULL::character varying,latitude varchar(255) NOT NULL DEFAULT NULL::character varying,longitude varchar(255) NOT NULL DEFAULT NULL::character varying,url varchar(500) NOT NULL DEFAULT NULL::character varying,PRIMARY KEY(id))")
# conn.commit()
# sys.exit()

for i in range(start,page):
    url = f"https://www.holidayiq.com/ajax/hotels/ooty/?page=1&sortType=1&railwayStation=&airport=&starRating=&hotelType=&amenity=&localities=&sightseeing=&holidayType=&withWhom=&rating=&pegs=&bha=&vr=&hotelchainId=&minPrice=0&maxPrice=28409&userMin=0&userMax=28409&isAjaxCall=1&destinationId=490&checkInDate=06%2F06%2F2020&checkOutDate=07%2F06%2F2020&destinationName=Ooty&pageUrl=%2F%2Fwww.holidayiq.com%2Fhotels%2Footy%2F&page={i}&_=1588696458331"

    html_ = r.get(url)
    dom = html.fromstring(html_.text)
    links = dom.xpath("//h2[@class='listing-hotel-star-heading']/a/@href")
    for i in links:
        html2 = r.get(i)
        dom2 = html.fromstring(html2.text)
        try:
            name = dom2.xpath("//h1[@class='detail-main-heading']/text()")
            if not name[0]:
                name[0]='NA'
        except:
            name = ['na']
        try:
            add = dom2.xpath("//span[@itemprop='addressLocality']/text()")
            if not add[0]:
                add[0]='NA'

        except:
            add = ['na']
        try:    
            cost = dom2.xpath("//meta[@itemprop='priceRange']/text()")
            if not cost[0]:
                cost[0]='NA'
        except:
            cost = ['na']
            
        try:
            rating = dom2.xpath("/html/body/div[2]/div[6]/div[2]/div[1]/div[1]/div/div[1]/div[1]/div[3]/div[1]/div/div/meta[4]/@content")
            if not rating[0]:
                rating[0]='NA'
        except:
            rating = ['na']
        try:
            no_of_review = dom2.xpath("/html/body/div[2]/div[6]/div[2]/div[1]/div[1]/div/div[1]/div[1]/div[3]/div[1]/div/div/meta[1]/@content")
            if not no_of_review[0]:
                no_of_review[0]='NA'
        except:
            no_of_review = ['na']
        try:
            lat = dom2.xpath("//input[@id='mapLat']/@value")
            if not lat[0]:
                lat[0]='NA'
        except:
            lat = ['na']
        try:
            lng = dom2.xpath("//input[@id='mapLong']/@value")
            if not lng[0]:
                lng[0]='NA'
        except:
            lng = ['na']

        insert = "INSERT INTO master.holidayiq_ooty(name, address, cost, rating, no_of_reviews, latitude, longitude,url) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"

        data = (name[0],add[0],cost[0],rating[0],no_of_review[0],lat[0],lng[0],i)

        print(data)
        cur.execute(insert,data)
        conn.commit()
