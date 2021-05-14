import io
import datetime
from os import error
import pandas
import requests
import zipfile
from bs4 import BeautifulSoup
# import rediscluster
import redis

PARSER = 'html.parser'

BHAV_HOME_URL = "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx"

USER_AGENT = "Chrome/90.0.4430.212"

HEADERS = {
    "User-Agent" : USER_AGENT
    }

REQ_TAG = "ContentPlaceHolder1_btnhylZip"

REDIS_URL = "rediss://default:rt04ubo8a2pvfcc7@bhav-data-do-user-9172163-0.b.db.ondigitalocean.com:25061"

# def connect(redis_host_endpoint):
#     startup_nodes = [{ "host": redis_host_endpoint, "port": "6379" }]
#     redis_pool = rediscluster.ClusterConnectionPool(max_connections=3, startup_nodes=startup_nodes, skip_full_coverage_check=True, decode_responses=True)
#     return redis_pool

# c = connect(REDIS_URL)
# r = rediscluster.RedisCluster(connection_pool=c)
store = redis.Redis.from_url(REDIS_URL)
print(store)
redis_obj = {}

def check_latest(file_name):

    # store.set('last', file_name)
    last_file_name = store.get("last").decode()
    print(last_file_name, file_name)

    if last_file_name != file_name:

        return True

    else:

        raise Exception({
            "status" : False,
            "message" : "Already synced the same file"
        })



def download_extract_zip(url):
    """
    Download a ZIP file and extract its contents in memory
    yields (filename, file-like object) pairs
    """
    try : 
        response = requests.get(url,headers=HEADERS)
        # print(response.content, response.headers)
        with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
            
            for zipinfo in thezip.infolist():
                with thezip.open(zipinfo) as thefile:
                    yield zipinfo.filename, thefile
    
    except Exception as e:
        print("here")
        raise Exception({
            "status" : False,
            "message" : e
        })

    

def get_zipped_data(url):

    bhav_data = {}
    file_name = ''
    try :

        for i in download_extract_zip(url):

            file_name = i[0]

            bhav_data = pandas.read_csv(i[1]).to_dict(orient = 'records')

            break
            
        return bhav_data, file_name

    except Exception as e:

        raise Exception( {
            "status" : False,
            "message" : e
        })


    
def get_bhav_zip_url():

    try : 
        response = requests.get(BHAV_HOME_URL, headers=HEADERS)
        response = response.text
        
        soup = BeautifulSoup(response, PARSER)
        link_tag = soup.find(id=REQ_TAG)
        
        return {
            "status":1,
            "data":link_tag['href']
            }

    except :

        raise Exception( {
            "status" : 0,
            "message" : "something went wrong while requesting asx url"
        })

def store_bhav_data(file_name, data):

    """
    store the file name first
    push each record in to the redis data storage
    """
    store.set('last', file_name)
    for record in data:

        print("record---------->",record)

        store.hmset(record['SC_CODE'], record)
   

    return True


def lambda_handler(event, context):

    try : 

        zip_url = get_bhav_zip_url()

        zip_url = zip_url.get('data')

        bhav_data, file_name = get_zipped_data(zip_url)

        check_latest(file_name)

        store_bhav_data(file_name, bhav_data)

        return {
            "status" : True,
            "message" : "successfully completed the ETL pipeline"
        }
    
    except Exception as e:

        return e


print(lambda_handler({},{}))


#https://www.bseindia.com/download/BhavCopy/Equity/EQ120521_CSV.ZIP

# username = default
# password = rt04ubo8a2pvfcc7
# host = bhav-data-do-user-9172163-0.b.db.ondigitalocean.com
# port = 25061