import pika
import pymongo
import secrets

def make_connection():
    credentials = pika.PlainCredentials(secrets.RABBITMQ_USER,
                                        secrets.RABBITMQ_PASSWD)
    connection_params = pika.ConnectionParameters(secrets.RABBITMQ_HOST,
                                                  secrets.RABBITMQ_PORT,
                                                  secrets.RABBITMQ_VHOST,
                                                  credentials=credentials)
    return pika.BlockingConnection(connection_params)

def db_connection():
    return pymongo.MongoClient(secrets.MONGO_HOST,
                                secrets.MONGO_PORT,
                                username=secrets.MONGO_WRITER_USER,
                                password=secrets.MONGO_WRITER_PASSWD)

def get_html_from_base(url):
    page_data = db_client['biographydb']['downloaded_pages'].find_one({'url': url})
    return page_data['body'], page_data['updated_at']

db_client = db_connection()


# simple_callback(self, msg) -> complex_callback(self, channel, method, properties, msg)
def amqp_callback(func):
    def wrapper(self, channel, method, properties, msg):
        print(f'called {func} with message `{msg}`')
        msg = msg.decode('utf-8')
        func(self, msg)
        channel.basic_ack(delivery_tag = method.delivery_tag)
    return wrapper

class AMQPWorkerHarness:
    def __init__(self):
        self.connection = make_connection()
        self.channel = self.connection.channel()
    #
    def seed(self):
        pass
    #
    def callback_list(self):
        return {}
    #
    def start(self):
        for queue_to_listen, stage_callback in self.callback_list().items():
            self.channel.queue_declare(queue=queue_to_listen)
            self.channel.basic_consume(queue=queue_to_listen,
                on_message_callback=stage_callback,
                auto_ack=False)
            print(f' [*] {queue_to_listen} is waiting for messages')
        self.seed()
        self.channel.start_consuming()
    #
    def send_message(self, queue, msg):
        self.channel.queue_declare(queue=queue)
        self.channel.basic_publish(exchange='', routing_key=queue, body=msg)


class Parser(AMQPWorkerHarness):
    @amqp_callback
    def parse_and_store(self, msg):
        data = self.parse(msg)
        self.send_message('store_to_db', data)
    #
    def parse(self, msg):
        raise NotImplementedError()


class PartisansParser(Parser):
    def callback_list(self):
        return {
            'partisans_parse_and_store': self.parse_and_store,
            'partisans_deferred_seed': self.deferred_seed,
        }
    #
    def seed(self):
        self.send_message('partisans_deferred_seed', 'start')
    #
    @amqp_callback
    def deferred_seed(self, msg):
        # for num in range(23601, 170000):
        for num in range(23601, 23650):
            # time.sleep(0.1 * random.randint(1,5))
            url = f'https://partizany.by/partisans/{num}/'
            msg = '\t'.join([url, 'partisans_parse_and_store'])
            self.send_message('to_download', msg)
        print('Seed completed')
    #
    def name_info(self, soup):
        name = soup.find("h1", attrs={'class': "info-book__title"}).text.strip()
        name = name.split(sep = " ", maxsplit=3)
        info = {}
        #
        try:
          info["surname"] = name[0]
        except IndexError:
          info["surname"] = ""
        #
        try:
          info["firstname"] = name[1]
        except IndexError:
          info["firstname"] = ""
        #
        try:
          info["patronym"] = name[2]
        except IndexError:
          info["patronym"] = ""
        #
        return info
    #
    def bio_info(self, soup):
        bio = soup.find("ul", attrs={'class': "info-book__descr"}).text
        info = {}
        #
        try:
          info["birthdate"] = re.search(r'\d{4}', bio).group().strip()
        except AttributeError:
          info["birthdate"] = ""
        #
        try:
          info["birthplace"] = re.search(r'(?<=Место\sрождения: \s).*', bio).group().strip()
        except AttributeError:
          info["birthplace"] = ""
        #
        try:
          info["nationality"] = re.search(r'(?<=Национальность:\s).*', bio).group().strip()
        except AttributeError:
          info["nationality"] = ""
        return info
    #
    def formation_info(self, soup):
        info = {}
        #
        try:
          info["time_period"] = []
          info["partisan_brigade"] = []
          info["partisan_detachment"] = []
          info["position"] = []
          formation_list = soup.find("div", attrs={'class': "formation__list"})
          for formation_item in formation_list.find_all("div", attrs={'class': "formation__item"}):
            info["time_period"].append(formation_item.find("div", attrs={'class': "formation__item-date"}).text)
            for item_descr in formation_item.find_all("ul", attrs={'class': "formation__item-descr"}):
              help = item_descr.find_all("li")
              info["partisan_brigade"].append(help[0].find('a').text)
              info["partisan_detachment"].append(help[1].find('a').text)
              info["position"].append(re.sub("^\nДолжность: ", "", help[2].text).strip())
        except:
          info["time_period"] = ""
          info["partisan_brigade"] = ""
          info["partisan_detachment"] = ""
          info["position"] = ""
        #
        return info
    #
    def award_info(self, soup):
        info = {}
        #
        try:
          info["award"] = []
          award = soup.find("div", attrs={'class': "info-book__rewards-list"})
          for rewards_item in award.find_all("div", attrs={'class': "info-book__rewards-item"}):
            info["award"].append(rewards_item.find("img")['alt'])
        except AttributeError:
          info["award"] = ""
        #
        try:
          info["award_nomination"] = []
          presented = soup.find("div", attrs={'class': "info-book__rewards-inline"})
          for item in presented.find_all("span", attrs={'class': "info-book__rewards-inline-text"}):
            info["award_nomination"].append(item.text)
        except AttributeError:
          info["award_nomination"] = ""
        #
        return info
    #
    def parse(self, msg):
        stored_url = msg
        html_text, updated_at = get_html_from_base(stored_url)
        print(get_partisan_info(html_text, url, updated_at)) # это надо будет передавать валидатору
        soup = BeautifulSoup(html_text)
        credentials = {
            "original_link": url,
            "retrieved_at": updated_at,
        }
        credentials.update(self.name_info(soup))
        credentials.update(self.bio_info(soup))
        credentials.update(self.formation_info(soup))
        credentials.update(self.award_info(soup))
        return credentials


parser = PartisansParser()
parser.start()







class SampleParser(Parser):
    def callback_list(self):
        return {
            'SampleParser_stage1': self.stage_1, # if necessary
            'SampleParser_stage2': self.stage_2, # if necessary
            'SampleParser_parse':  self.parse_and_store,    # obligatory
        }
    #
    def seed(self):
        # For downloading
        output_queue = 'SampleParser_stage1'
        for i in range(1000):
            url = f'https://some.site.org/url/{i}'
            self.send_message('to_download', f'{url}\t{SampleParser_stage1}\tsend_content')
            # or
            # self.send_message('to_download', f'{url}\t{SampleParser_stage1}\tsend_url')

        # For table loading
        with open('table.tsv') as f:
            reader = csv.DictReader(f)
            for line in reader:
                self.send_message('SampleParser_parse', line)
    #
    def stage_1(self, msg):
        # do smth with msg
        self.send_message('SampleParser_stage2', 'some_data')
    #
    def stage_2(self, msg):
        # do smth with msg
        self.send_message('SampleParser_parse', 'document_text')
    #
    # Obligatory
    def parse(self, msg_or_url):
        some_json
        self.send_message('SampleParser_parse', 'document_text')


if __name__ == '__main__':
    parser = PartisansParser()
    parser.start()
