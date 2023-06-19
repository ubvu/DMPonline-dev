import requests
from config import CLIENT_SECRET
from loguru import logger

DMP_ONLINE_URL = 'https://dmponline.vu.nl/api/v0'

DEFAULT_HEADERS = {
    'Content-Type': 'application/json',
    'Authorization': f'Token token={CLIENT_SECRET}'
}

logger.add(r'U:\Werk\Data Management\Python\Files\DMP_Online\API_0v1.log', backtrace=True, diagnose=True, rotation="10 MB", retention="12 months")
logger.debug('The script was running today.')
@logger.catch()

def retrieve_plans(identifier=None):
    """ retrieves dmps for a given id (or all)
    :param identifier: plan number for which to retrieve full-text (None=all)
    :returns generator for page requests
    """
    # api used to provide a pages count, but that's not the case in current version
    more_pages = True
    page = 1
    while more_pages:
        params = {'page': page}
        if identifier:
            params['plan'] = identifier
        data = request_api(params)
        if data:
            print(f'retrieved page {page}')
            yield data
            page += 1
        else:
            more_pages = False
            print(f'no more pages')


def request_api(params):
    target = f'{DMP_ONLINE_URL}/plans'
    resp = requests.get(target, headers=DEFAULT_HEADERS, params=params)

    if resp.status_code == 200:
        data = resp.json()
        if len(data) == 0:
            return None  # [] returned when out of pages
        else:
            return data
    else:
        print('api error')
        print(resp.status_code)
        print(resp.text)
        return None
