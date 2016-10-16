import logging
import json
from spyne import Application, srpc, ServiceBase, Iterable, UnsignedInteger, \
    String
from datetime import datetime
from spyne.protocol.json import JsonDocument
from spyne.protocol.http import HttpRpc
from spyne.server.wsgi import WsgiApplication
from urllib2 import Request, urlopen, URLError
import requests
import re
import operator
from address import AddressParser, Address


class HelloWorldService(ServiceBase):
    @srpc(String, String, String, _returns=Iterable(String))
    def checkcrime(lat, lon, radius):
        payload = {'lat': '37.334164', 'lon': '-121.884301', 'radius':'0.05', 'key':'.'}
        payload['lat'] = lat
        payload['lon'] = lon
        payload['radius'] = radius

        try:
            r = requests.get('https://api.spotcrime.com/crimes.json', params=payload)
            print r.url
            responseText = r.text
            crimes = re.findall(r'"cdid":\w*,"\w*":"\w*","\w*":"\d*/\d*/\d* \w*:\w*\s*\w*","\w*":"[\w*\s*]*"', responseText)
            totalCrimes = 0
            crimeType = {}
            eventTime = {'12:01am-3am': 0, '3:01am-6am': 0, '6:01am-9am': 0, '9:01am-12noon': 0, '12:01pm-3pm': 0, '3:01pm-6pm': 0, '6:01pm-9pm': 0, '9:01pm-12midnight': 0}
            tmMidNght  = datetime.strptime(' 12:00 AM', ' %I:%M %p')
            tm3am  = datetime.strptime(' 3:00 AM', ' %I:%M %p')
            tm6am  = datetime.strptime(' 6:00 AM', ' %I:%M %p')
            tm9am  = datetime.strptime(' 9:00 AM', ' %I:%M %p')
            tm12pm  = datetime.strptime(' 12:00 PM', ' %I:%M %p')
            tm3pm  = datetime.strptime(' 3:00 PM', ' %I:%M %p')
            tm6pm  = datetime.strptime(' 6:00 PM', ' %I:%M %p')
            tm9pm  = datetime.strptime(' 9:00 PM', ' %I:%M %p')
            tm12pm  = datetime.strptime(' 12:00 PM', ' %I:%M %p')
            streets = {}
            for crime in crimes:
                totalCrimes += 1
                typeExacct = re.findall(r':"\w*"',crime)
                tm = re.findall(r"['\"](.*?)['\"]", typeExacct[0])
                if crimeType.has_key(tm[0]):
                    crimeType[tm[0]] += 1
                else:
                    crimeType[tm[0]] = 1
                
                addr = re.findall(r'"address":"[\s*\w*]*"',crime)
                address = re.findall(r':"[\s*\w*]*"',addr[0])
                street = re.findall(r"['\"](.*?)['\"]", address[0])
                ap = AddressParser()
                streetName = ap.parse_address(street[0])
                streetKey = ""
                if streetName.street_prefix is not None:
                    streetKey += streetName.street_prefix
                if streetName.street is not None:
                    if streetKey is not None:
                        streetKey += " "
                    streetKey += streetName.street
                if streetName.street is not None:
                    if streetKey is not None:
                        streetKey += " "
                    streetKey += streetName.street_suffix
                
                if streets.has_key(streetKey):
                    streets[streetKey] += 1
                else:
                    streets[streetKey] = 1
                sorted_streets = sorted(streets.items(), key=operator.itemgetter(1), reverse=True)
                
                timeDate = re.findall(r'"date":"\d*/\d*/\d* \w*:\w*\s*\w*"',crime)
                timeExact = re.findall(r'\s\w*',timeDate[0])
                time2 = re.findall(r'\s\w*:\w*',timeDate[0])
                crmTime = re.findall(r'\s\w*:\w*\s\w*',timeDate[0])
                date_object = datetime.strptime(crmTime[0], ' %I:%M %p')
                if date_object.time() > tmMidNght.time():
                    if date_object.time() <= tm3am.time():
                        eventTime['12:01am-3am'] += 1
                    elif date_object.time() <= tm6am.time():
                        eventTime['3:01am-6am'] += 1
                    elif date_object.time() <= tm9am.time():
                        eventTime['6:01am-9am'] += 1
                    elif date_object.time() <= tm12pm.time():
                        eventTime['9:01am-12noon'] += 1
                    elif date_object.time() <= tm3pm.time():
                        eventTime['12:01pm-3pm'] += 1
                    elif date_object.time() <= tm6pm.time():
                        eventTime['3:01pm-6pm'] += 1
                    elif date_object.time() <= tm9pm.time():
                        eventTime['6:01pm-9pm'] += 1
                    else:
                        eventTime['9:01pm-12midnight'] += 1
                else:
                    eventTime['9:01pm-12midnight'] += 1
            
            dangerours_streets = [sorted_streets[0][0], sorted_streets[1][0], sorted_streets[2][0]]
            data = {}
            data['total_crime'] = totalCrimes
            data['the_most_dangerous_streets'] = dangerours_streets
            data['crime_type_count'] = crimeType
            data['event_time_count'] = eventTime
            yield data
            
        except URLError, e:
            data = {}
            data['status'] = "No data available. Got an error."
            data['error code'] = e
            yield data


if __name__ == '__main__':

    from wsgiref.simple_server import make_server

    application = Application([HelloWorldService], 'spyne.crimes.report.http',
        in_protocol=HttpRpc(validator='soft'),
        out_protocol=JsonDocument(ignore_wrappers=True),
    )
    wsgi_application = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_application)

    logging.info("listening to http://127.0.0.1:8000")
    logging.info("wsdl is at: http://localhost:8000/?wsdl")

    server.serve_forever()