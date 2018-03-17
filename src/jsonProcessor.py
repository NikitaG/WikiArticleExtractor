import logging
import time
import ujson
import cProfile

cities = {'Q515', 'Q532', 'Q486972'}
countries = {'Q6256', 'Q3624078'}
regions = {'Q35657', 'Q28872924'}


class JsonProcessor:
    def __init__(self, id):
        self.__id = id

        self.process = None

        self.subclasses = 0
        #self.profile =  cProfile.Profile()
        #self.profile.enable()
        pass

    def execute(self, jobs_queue, output_queue):
        logging.info("JsonProcessor #{} is starting...".format(self.__id))

        while True:
            jobs = jobs_queue.get()

            if jobs:
                for job in jobs:
                    job_id = job[0]
                    line = job[1].decode('utf-8')

                    if not line or line[0] != "{": continue

                    obj = self.convert(line[:-2])
                    if not obj:
                        continue

                    try:
                        location = self.extract_info(obj)
                    except Exception as error:
                        location = None
                        logging.error("Error in extraction_info: {}".format(error))

                    if not location:
                        continue

                    output_queue.put(('location', location))

                    if self.subclasses % 1000 == 0:
                        logging.info("Total subclasses: {}.".format(self.subclasses))
            else:
                break
        #self.profile.disable()
        #self.profile.print_stats("tottime")
        logging.info("JsonProcessor #{} completed. Total subclasses: {}".format(self.__id, self.subclasses))

    def extract_info(self, json):
        if 'P279' in json['claims']:
            self.subclasses += 1

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
                logging.error('Invalid location: '+ str(coordinates) )

        if location['globe'] != 'http://www.wikidata.org/entity/Q2':
            return None

        heritage = 'P1435' in json['claims']
        images = self.__claim_values(json, "P18")
        imagesList = list(images) if images else None

        p31 = self.__extract_claim_value_id(json, "P31")
        p3134 = self.__claim_values(json, "P3134")
        tourist_attraction, archaeological_sites = 'Q570116' in p31, 'Q839954' in p31
        city, region, country = bool(cities & p31), bool(regions & p31), bool(countries & p31)
        trip_advisor_id = list(p3134) if p3134 else None

        return {'type': json['type'], 'id': json['id'], 'title': title, 'description': decription, 'images': imagesList,
                'location': location, 'heritage': heritage,
                'tourist_attraction': tourist_attraction, 'archaeological_sites': archaeological_sites, 'city': city, 'region': region, 'country': country, 'trip_advisor_id': trip_advisor_id}

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
            if json_string[:14] != '{"type":"item"':
                return None

            js = ujson.loads(json_string)
            return js
        except Exception as error:
            logging.error("Couldn't convert json: {}".format(error))
            return None
