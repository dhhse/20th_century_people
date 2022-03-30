import pymongo

def db_connection(secrets):
    return pymongo.MongoClient(secrets['MONGO_HOST'],
                                secrets['MONGO_PORT'],
                                username=secrets['MONGO_WRITER_USER'],
                                password=secrets['MONGO_WRITER_PASSWD'])

def get_html_from_base(db_client, url):
    page_data = db_client['biographydb']['downloaded_pages'].find_one({'url': url})
    return page_data['body'], page_data['updated_at']
