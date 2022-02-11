import requests
from config import CLIENT_SECRET

DMP_ONLINE_URL = 'https://dmponline.vu.nl/api/v0'

DEFAULT_HEADERS = {
    'Content-Type': 'application/json',
    'Authorization': f'Token token={CLIENT_SECRET}'
}


def retrieve_plans(date=None):
    """ retrieves DMPs for a given day (or all)
    :param date: day for which to retrieve plans (None=all)
    :returns generator for page requests
    """
    # api used to provide a pages count, but that's not the case in current version
    more_pages = True
    page = 0
    while more_pages:
        params = {'page': page}
        if date:
            params['updated_after'] = date
            params['updated_before'] = date
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
        return None

