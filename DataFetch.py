# based on John Hopkins CSSE
# converted the json to excel using : https://json-csv.com/
# rapid api : https://rapidapi.com/axisbits-axisbits-default/api/covid-19-statistics/pricing

import requests
import json
import settings


def fetch_json():
    url = settings.url

    querystring = {"iso":"USA"}

    headers = {
        'x-rapidapi-host': settings.rapidapihost,
        'x-rapidapi-key': settings.apikey
        }
    response = requests.request("GET", url, headers=headers, params=querystring)
    with open('covid_json.json','w') as json_data:
        json_data.write(response.text)
        return 'covid_json.json'


def create_insert_statements(json_file):
    with open(json_file,'r') as f:
        with open('covid_inserts.sql','w') as fw:
            stats_dict = json.load(f)
            print(stats_dict.keys())
            for item in stats_dict['data']:
                print('item --> {}'.format(item))
                print('region --> {}'.format(item['region']['province']))
                insert_state = """insert into covid_states(state,total_number_of_cases,total_deaths,total_recovered,active_cases,
                      new_cases,new_deaths,new_recovers,updated_at) values ("{state}",{cases},{total_deaths},
                      {total_recovered},{active},{new_cases},{new_deaths},{new_recovery},'{updated_at}') """.format(state=item['region']['province'],
                                                                                          cases=item['confirmed'],
                                                                                          total_deaths=item['deaths'],
                                                                                          total_recovered=item['recovered'],
                                                                                          active=item['active'],
                                                                                          new_cases=item['confirmed_diff'],
                                                                                          new_deaths=item['deaths_diff'],
                                                                                          new_recovery=item['recovered_diff'],
                                                                                          updated_at=item['last_update'])
                fw.write(insert_state + ';' + '\n')
                for city in item['region']['cities']:

                    active_cases = city['confirmed'] - city['deaths']
                    insert_city ="""Insert into covid_cities(state,city,total_number_of_cases,total_deaths,
                    active_cases,new_cases,new_deaths,udpated_at) values ("{state}","{city}",{confirmed_cases},
                    {deaths},{active_cases},{new_cases},{new_death},'{insert_time}')
                    """.format(state = item['region']['province'],
                            city=city['name'],confirmed_cases=city['confirmed'],deaths=city['deaths'],
                            active_cases=active_cases,new_cases=city['confirmed_diff'],new_death=city['deaths_diff'],
                            new_recovery='',insert_time=city['last_update'])

                    fw.write(insert_city + ';' + '\n')
                print('Done!!!')
                return 'covid_inserts.sql'


if __name__ == '__main__':

    # call api and fetch the json
    json_data = fetch_json()

    # create insert scripts from the json
    inc = create_insert_statements('covid_json.json')

    # insert into ventana tables
    # 1. Connect to the DB
    # 2. truncate the existing tables covid_states and covid_cities
    # 3. load the inc file into ventana

