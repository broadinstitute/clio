#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import print_function
import requests
import os
import time

clio_http_scheme = os.environ.get('CLIO_HTTP_SCHEME', 'http')
clio_http_host = os.environ.get('CLIO_HTTP_HOST', 'localhost')
clio_http_port = os.environ.get('CLIO_HTTP_PORT', 8080)
clio_http_uri = '{0}://{1}:{2}'.format(clio_http_scheme, clio_http_host, clio_http_port)


def test_wait_for_clio():
    count = 0
    print("waiting for " + clio_http_uri)
    while count < 20:
        try:
            r = requests.get(clio_http_uri + '/health')
            js = r.json()
            clio_status = js['clio']['status']
            elasticsearch = js['elasticsearch']
            elasticsearch_status = elasticsearch['status']
            elasticsearch_data_nodes = elasticsearch['dataNodes']
            if clio_status == 'started' and elasticsearch_status == 'green' and elasticsearch_data_nodes > 1:
                print("connected to clio. Elasticsearch status %s, running %s nodes." %
                      (elasticsearch_status, elasticsearch_data_nodes))
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(3)
        count = count + 1


def test_version():
    r = requests.get(clio_http_uri + '/version')
    js = r.json()
    assert 'version' in js


def test_health():
    r = requests.get(clio_http_uri + '/health')
    js = r.json()
    assert js['clio']['status'] == 'started'
    assert js['elasticsearch']['status'] == 'green'
    assert js['elasticsearch']['nodes'] > 1
    assert js['elasticsearch']['dataNodes'] > 1


def test_bad_method():
    r = requests.post(clio_http_uri + '/health')
    js = r.json()
    assert r.status_code == requests.codes.method_not_allowed
    assert js['rejection'] == 'HTTP method not allowed, supported methods: GET'


def test_bad_path():
    r = requests.get(clio_http_uri + '/badpath')
    js = r.json()
    assert r.status_code == requests.codes.not_found
    assert js['rejection'] == 'The requested resource could not be found.'


def test_authorization():
    woExpect = { # expected results from testWo
        'OIDC_access_token':     'forbidden',
        'OIDC_CLAIM_expires_in': 'forbidden',
        'OIDC_CLAIM_email':      'forbidden',
        'OIDC_CLAIM_sub':        'ok',
        'OIDC_CLAIM_user_id':    'ok'
    }
    def makeHeaders(woExpect): # derive headers from woExpect
        result = { key : key for key in woExpect.keys() }
        oks = [key for key, value in woExpect.items() if value == 'ok']
        id = ' or '.join(oks)
        for ok in oks: result[ok] = id
        return result
    headers = makeHeaders(woExpect)
    authUrl = clio_http_uri + '/authorization'
    def gas(headers):
        response = requests.get(authUrl, headers=headers)
        json = response.json()
        code = response.status_code
        result = requests.get(authUrl, headers=headers).status_code
        return result
    def wo(header): # a copy of headers without header
        result = dict(headers)
        del result[header]
        return result
    def testWo(header, status):
        print('testWo: ', header, " = ", status)
        assert gas(wo(header)) == requests.codes[status]
    for header, expect in woExpect.items(): testWo(header, expect)


if __name__ == '__main__':
    test_wait_for_clio()
    test_version()
    test_health()
    test_bad_method()
    test_bad_path()
    test_authorization()
    print('tests passed')
