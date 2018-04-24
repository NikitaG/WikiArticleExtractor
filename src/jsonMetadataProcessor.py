import logging
import time
import ujson
import cProfile

exclude = {'Q5','Q515', 'Q577', 'Q532', 'Q486972','Q6256', 'Q3624078','Q35657', 'Q28872924', 'Q47018901', 'Q10373548',
           'Q7854', 'Q33146843', 'Q16521','Q4167410', 'Q11424', 'Q482994', 'Q484170', 'Q747074', 'Q4167836', 'Q13406463',
           'Q3863', 'Q11266439'}


class JsonMetadataProcessor:
    def __init__(self, id):
        self.__id = id

        self.process = None

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
                        metadata = self.extract_info(obj)
                    except Exception as error:
                        metadata = None
                        logging.error("Error in extraction_info: {}".format(error))

                    if not metadata:
                        continue

                    output_queue.put(('metadata', metadata))
            else:
                break
        #self.profile.disable()
        #self.profile.print_stats("tottime")
        logging.info("JsonProcessor #{} completed.".format(self.__id))

    def extract_info(self, json):
            # if json['id'] not in exclude:
            #     return None

        if 'en' not in json['labels']:
            return None

        instances = self.__extract_claim_value_id(json, "P31")
        # if bool(instances & exclude):
        #     return None

        subclasses = self.__extract_claim_value_id(json,"P279")

        label = json['labels']['en']['value'] if 'en' in json['labels'] else None
        labelRu = json['labels']['ru']['value'] if 'ru' in json['labels'] else None

        descr = json['descriptions']['en']['value'] if 'en' in json['descriptions'] else None
        descrRu = json['descriptions']['ru']['value'] if 'ru' in json['descriptions'] else None

        return {'id': json['id'], 'label_en': label, 'label_ru': labelRu, 'description_en': descr, 'description_ru': descrRu, 'instances': list(instances), 'subclasses': list(subclasses)}

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
