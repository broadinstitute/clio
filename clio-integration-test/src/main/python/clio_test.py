#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import print_function

import os
import requests
import time
import uuid


def get_environ_uri(prefix, scheme_default='http', host_default='localhost', port_default=80):
    scheme = os.environ.get(prefix + '_HTTP_SCHEME', scheme_default)
    host = os.environ.get(prefix + '_HTTP_HOST', host_default)
    port = os.environ.get(prefix + '_HTTP_PORT', port_default)
    return '{0}://{1}:{2}'.format(scheme, host, port)


clio_http_uri = get_environ_uri('CLIO', port_default=8080)
elasticsearch_http_uri = get_environ_uri('ELASTICSEARCH', port_default=9200)


def test_wait_for_clio():
    count = 0
    print("waiting for " + clio_http_uri)
    while count < 20:
        try:
            r = requests.get(clio_http_uri + '/health')
            js = r.json()
            clio_status = js['clio']
            search_status = js['search']
            if clio_status == 'Started' and search_status == 'OK':
                print("connected to clio.")
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(3)
        count = count + 1
    raise AssertionError("unable to connect to " + clio_http_uri)


def test_version():
    r = requests.get(clio_http_uri + '/version')
    js = r.json()
    assert 'version' in js


def test_health():
    r = requests.get(clio_http_uri + '/health')
    js = r.json()
    assert js['clio'] == 'Started'
    assert js['search'] == 'OK'


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


def __assert_expected_fields(expected_fields, mapping_properties):
    for (field_name, field_type) in expected_fields.items():
        assert mapping_properties[field_name]['type'] == field_type
    assert len(expected_fields) == len(mapping_properties)


def test_read_group_mapping():
    r = requests.get(elasticsearch_http_uri + '/read_group/_mapping/default')
    js = r.json()
    mappings = js['read_group']['mappings']['default']
    assert mappings['dynamic'] == 'false'
    expected_fields = {
        'analysis_type': 'keyword',
        'bait_intervals': 'keyword',
        'data_type': 'keyword',
        'flowcell_barcode': 'keyword',
        'individual_alias': 'keyword',
        'initiative': 'keyword',
        'lane': 'integer',
        'lc_set': 'keyword',
        'library_name': 'keyword',
        'library_type': 'keyword',
        'machine_name': 'keyword',
        'molecular_barcode_name': 'keyword',
        'molecular_barcode_sequence': 'keyword',
        'paired_run': 'boolean',
        'product_family': 'keyword',
        'product_name': 'keyword',
        'product_order_id': 'keyword',
        'product_part_number': 'keyword',
        'project': 'keyword',
        'read_structure': 'keyword',
        'research_project_id': 'keyword',
        'research_project_name': 'keyword',
        'root_sample_id': 'keyword',
        'run_date': 'date',
        'run_name': 'keyword',
        'sample_alias': 'keyword',
        'sample_gender': 'keyword',
        'sample_id': 'keyword',
        'sample_lsid': 'keyword',
        'sample_type': 'keyword',
        'target_intervals': 'keyword',
        'ubam_md5': 'keyword',
        'ubam_path': 'keyword',
        'ubam_size': 'long',
    }
    __assert_expected_fields(expected_fields, mappings['properties'])


def test_authorization():
    withoutExpect = { # expected results from testWithout
        'OIDC_access_token':     'forbidden',
        'OIDC_CLAIM_expires_in': 'forbidden',
        'OIDC_CLAIM_email':      'forbidden',
        'OIDC_CLAIM_sub':        'ok',
        'OIDC_CLAIM_user_id':    'ok'
    }
    def mockHeaders(withoutExpect): # derive mock headers from withoutExpect
        result = { key : key for key in withoutExpect.keys() }
        oks = [key for key, value in withoutExpect.items() if value == 'ok']
        id = ' or '.join(oks)
        for ok in oks: result[ok] = id
        result['OIDC_CLAIM_expires_in'] = str(1234567890)
        return result
    headers = mockHeaders(withoutExpect)
    authUrl = clio_http_uri + '/authorization'
    def getAuthStatus(headers):
        return requests.get(authUrl, headers=headers).status_code
    def without(header): # a copy of headers without header
        result = dict(headers)
        del result[header]
        return result
    def testWithout(header, status):
        assert getAuthStatus(without(header)) == requests.codes[status]
    for header, expect in withoutExpect.items(): testWithout(header, expect)


def test_read_group_metadata():
    id = str(uuid.uuid4()).replace('-', '')
    library = 'library' + id
    upsert = {'project': 'testProject'}
    r = requests.post(clio_http_uri + '/readgroup/metadata/v1/barcode1/1/' + library, json=upsert)
    js = r.json()
    assert js == {}
    query = {'library_name': library}
    r = requests.post(clio_http_uri + '/readgroup/query/v1', json=query)
    js = r.json()
    assert len(js) == 1
    assert js[0] == {
        'flowcell_barcode': 'barcode1',
        'lane': 1,
        'library_name': library,
        'project': 'testProject',
    }


# I don't know how to derive the required fields.
#
def test_json_schema():
    def expected():
        schemas = { 'keyword': {"type": "string" },
                    'boolean': {"type": "boolean"},
                    'integer': {"type": "integer", "format": "int32"},
                    'long':    {"type": "integer", "format": "int64"},
                    'date':    {"type": "string" , "format": "date-time"} }
        response = requests.get(elasticsearch_http_uri + '/read_group/_mapping/default')
        mapping = response.json()['read_group']['mappings']['default']['properties']
        properties = { key: schemas[value['type']] for key, value in mapping.items() }
        return { 'type': 'object',
                 'required': [ 'flowcell_barcode', 'lane', 'library_name' ],
                 'properties': properties }
    response = requests.get(clio_http_uri + '/readgroup/schema/v1')
    result = response.json()
    assert result == expected()


def test_read_group_metadata_v2():
    id = str(uuid.uuid4()).replace('-', '')
    library = 'library' + id
    upsert = {'project': 'testProject'}
    r = requests.post(clio_http_uri + '/readgroup/metadata/v2/barcode1/1/' + library, json=upsert)
    js = r.json()
    assert js == {}
    query = {'library_name': library}
    r = requests.post(clio_http_uri + '/readgroup/query/v2', json=query)
    js = r.json()
    assert len(js) == 1
    assert js[0] == {
        'flowcell_barcode': 'barcode1',
        'lane': 1,
        'library_name': library,
        'project': 'testProject',
    }


if __name__ == '__main__':
    test_wait_for_clio()
    test_version()
    test_health()
    test_bad_method()
    test_bad_path()
    test_read_group_mapping()
    test_authorization()
    test_read_group_metadata()
    test_json_schema()
    test_read_group_metadata_v2()
    print('tests passed')
