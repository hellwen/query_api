#coding=utf-8

import unittest
import json

'''
import json, requests

url = 'http://maps.googleapis.com/maps/api/directions/json'

params = dict(
    origin='Chicago,IL',
    destination='Los+Angeles,CA',
    waypoints='Joplin,MO|Oklahoma+City,OK',
    sensor='false'
)

resp = requests.get(url=url, params=params)
data = json.loads(resp.text)
'''

class CaseAggregate(unittest.TestCase):

    def setUp(self):
        self.f = open("test/data/case_aggregate.json")

    def tearDown(self):
        self.f.close()

    def test1(self):
        from query import Query

        s = json.load(self.f)
        e = Query(s)
        resp = e.parse()

        print("\n-----" + self.__class__.__name__ + "\n")
        print(resp)
        print("")
        dset = e.execute()
        print(dset["title"])
        for row in dset["data"]:
            print(row)

if __name__ == '__main__':
    unittest.main()
