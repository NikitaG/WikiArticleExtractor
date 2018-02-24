import bz2
import json
import os
import psycopg2
import time
from collections import namedtuple


def _json_object_hook(d): return namedtuple('X', d.keys())(*d.values())


def json2obj(data): return json.loads(data)

conn = psycopg2.connect(host="localhost",database="articleLocation", user="postgres", password="123456")
sql = """INSERT INTO public."ArticleLocationTmp"(type, "wikiId", title, longitude, latitude, "precision", heritage)
	VALUES (%s, %s, %s, %s, %s, %s, %s);"""

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        #
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(host="localhost",database="articleLocation", user="postgres", password="123456")

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


f = bz2.BZ2File('wikidata.json.bz2', 'r')
if os.path.exists("error.txt"):
    os.remove("error.txt")

def logError(error, json, coordinates = None):
    pass
    #with open("error.txt", "a") as myfile:
    #    myfile.write("{} {} {}\n".format('Error '+ error, str(json)[:50], coordinates or ""))

def logInfo(json):
    print('Info', json)
    pass


def processFile(file):
    counter = 0
    locations = 0
    now = time.time()
    for line in file:

        counter += 1
        line = line.decode('utf-8')

        if counter % 100 == 0:
            print("Rows processed:", counter, counter / (time.time() - now), time.time() - now)

        json = convert(line)
        if not json: continue

        try:
            obj = extract(json())
        except:
            obj = None

        if not obj: continue

        locations += 1


        #print(line)
        yield obj

    print("Total:", counter, "\t with locations: ", locations)


def convert(json):
    if not json or json[0] != "{": return None

    return lambda: json2obj(json[:-2])


def extract(json):
    if 'labels' not in json or \
            'en' not in json['labels']:
        logError("Label not found", json)
        return None
    if 'P625' not in json['claims']:
        #logInfo(json)
        return None

    title = json['labels']['en']['value']

    coordinates = json['claims']['P625']

    location = None
    if(len(coordinates) > 1):
        for c in coordinates:
            if location is None or (c['mainsnak']['datavalue']['value']['precision'] or 0.1) < (location['precision'] or 0.1):
                location = c['mainsnak']['datavalue']['value']
        logError("More than one coordinate", json, coordinates)
        #print("Multiple coordinates: ", coordinates)
    else:
        location = coordinates[0]['mainsnak']['datavalue']['value']

    heritage = 'P1435' in json['claims']

    return {'type': json['type'], 'id': json['id'], 'title': title, 'location': location, 'heritage': heritage }

def addData(obj):

    # create a new cursor
    cur = conn.cursor()
    # execute the INSERT statement
    # (type, "wikiId", title, longitude, latitude, "precision", heritage)
    cur.execute(sql, (obj['type'], obj['id'], obj['title'], obj['location']['longitude'], obj['location']['latitude'], obj['location']['precision'], obj['heritage']))

    # commit the changes to the database
    conn.commit()
    pass


i = 0

for line in processFile(f):
    i += 1
    #if i < 58129:
    #    continue

    #addData(line)

    if i % 1000 == 0:
        print("Row added", i)

    #if i == 5: break;

print("Row added", i)

# st = "\n".join([ json2obj(x[:-2]) for x in lines if x])

# print(st["claims"]["P625"][0])
