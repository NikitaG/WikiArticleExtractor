import logging
import time
import ujson


class JsonProcessor:
    def __init__(self, id):
        self.__id = id

        self.process = None
        pass

    def execute(self, jobs_queue, output_queue):
        logging.info("JsonProcessor #{} is starting...".format(self.__id))

        while True:
            job = jobs_queue.get()
            if job:
                job_id = job[0]
                line = job[1].decode('utf-8')

                if not line or line[0] != "{": continue

                obj = self.convert(line[:-2])
                if not obj: continue

                try:
                    location = self.extract_info(obj)
                except Exception as error:
                    logging.error("Error in extraction_info: " + error)
                if not location: continue

                output_queue.put(location)

                # tim# e.sleep(0.001)
                # logging.debug("Processing job #{} in processor #{}".format(job[0], self.__id))
                pass
            else:
                break

        logging.info("JsonProcessor #{} completed.".format(self.__id))

    def extract_info(self, json: object) -> object:
        if 'labels' not in json or \
                'en' not in json['labels']:
            # logging.debug("Label not found", json)
            return None
        if 'P625' not in json['claims']:
            # coordinates not found
            return None

        title = json['labels']['en']['value']
        decription = json['descriptions']['en']['value'] if 'en' in json['descriptions'] else None

        coordinates = json['claims']['P625']

        location = None
        if (len(coordinates) > 1):
            for c in coordinates:
                if location is None or (c['mainsnak']['datavalue']['value']['precision'] or 0.1) < (
                        location['precision'] or 0.1):
                    location = c['mainsnak']['datavalue']['value']
            # logging.debug("More than one coordinate found.")
        else:
            if 'datavalue' in coordinates[0]['mainsnak']:
                location = coordinates[0]['mainsnak']['datavalue']['value']
            else:
                logging.error('Invalid location: '+ coordinates[0]['mainsnak'] )

        if location['globe'] != 'http://www.wikidata.org/entity/Q2':
            return None

        heritage = 'P1435' in json['claims']
        images = self.__claim_values(json, "P18")
        image = images[0] if images else None

        p31 = self.__extract_claim_value_id(json, "P31")
        tourist_attraction, archaeological_sites, city, country = 'Q570116' in p31, 'Q839954' in p31, 'Q515' in p31, 'Q159' in p31

        return {'type': json['type'], 'id': json['id'], 'title': title, 'description': decription, 'image': image,
                'location': location, 'heritage': heritage,
                'tourist_attraction': tourist_attraction, 'archaeological_sites': archaeological_sites, 'city': city, 'country': country}

    def __extract_claim_value_id(self, json, claim_id):
        if claim_id not in json['claims']:
            return set()

        return set([x['mainsnak']['datavalue']['value']['id'] for x in json['claims'][claim_id]])

    def __claim_values(self, json, claim_id):
        if claim_id not in json['claims']:
            return []

        return [x['mainsnak']['datavalue']['value'] for x in json['claims'][claim_id]]

    def convert(self, json_string):
        try:
            json = ujson.loads(json_string)
            return json
        except Exception as error:
            logging.error("Couldn't convert json: {}".format(error))
            return None
