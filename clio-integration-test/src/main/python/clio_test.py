#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import print_function

import os
import requests
import time
import uuid


def get_environ_url(prefix, scheme='http', host='localhost', port=80):
    s = os.environ.get(prefix + '_HTTP_SCHEME', scheme)
    h = os.environ.get(prefix + '_HTTP_HOST', host)
    p = os.environ.get(prefix + '_HTTP_PORT', port)
    return '{0}://{1}:{2}'.format(s, h, p)

def prefix_url(prefix, *args):
    path = [prefix]
    path.extend([str(arg) for arg in args])
    return '/'.join(path)

def elasticsearch_url(*args):
    return prefix_url(get_environ_url('ELASTICSEARCH', port=9200), *args)

def clio_url(*args):
    return prefix_url(get_environ_url('CLIO', port=8080), *args)

def readgroup_v1_url(*args):
    return clio_url('api', 'v1', 'readgroup', *args)


def get_cluster_health():
    response = requests.get(elasticsearch_url('_cluster', 'health'))
    return response.json()['status']


def test_wait_for_clio(allowYellowHealth=False):
    count = 0
    print('waiting for', clio_url())
    while count < 20:
        try:
            js = requests.get(clio_url('health')).json()
            clio_status = js['clio']
            search_status = js['search']
            if allowYellowHealth and 'yellow' == get_cluster_health():
                print('WARNING: test_wait_for_clio() allows yellow health.')
                search_status = 'OK'
            if clio_status == 'Started' and search_status == 'OK':
                print('connected to clio')
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(3)
        count = count + 1
    raise AssertionError("unable to connect to " + clio_url())


def test_version():
    assert 'version' in requests.get(clio_url('version')).json()


def test_health(allowYellowHealth=False):
    js = requests.get(clio_url('health')).json()
    assert js['clio'] == 'Started'
    if allowYellowHealth:
        print('WARNING: test_health() allowing yellow cluster health.')
        assert 'yellow' == get_cluster_health()
    else:
        assert js['search'] == 'OK'


def test_bad_method():
    response = requests.post(clio_url('health'))
    js = response.json()
    assert response.status_code == requests.codes.method_not_allowed
    assert js['rejection'] == 'HTTP method not allowed, supported methods: GET'


def test_bad_path():
    response = requests.get(clio_url('badpath'))
    js = response.json()
    assert response.status_code == requests.codes.not_found
    assert js['rejection'] == 'The requested resource could not be found.'


def test_read_group_mapping():
    keywords = ['flowcell_barcode',
                'library_name',
                'location',
                'analysis_type',
                'bait_intervals',
                'data_type',
                'individual_alias',
                'initiative',
                'lc_set',
                'library_type',
                'machine_name',
                'molecular_barcode_name',
                'molecular_barcode_sequence',
                'product_family',
                'product_name',
                'product_order_id',
                'product_part_number',
                'project',
                'read_structure',
                'research_project_id',
                'research_project_name',
                'root_sample_id',
                'run_name',
                'sample_alias',
                'sample_gender',
                'sample_id',
                'sample_lsid',
                'sample_type',
                'target_intervals',
                'ubam_md5',
                'ubam_path']
    expected = {k: 'keyword' for k in keywords}
    expected.update({k: 'integer' for k in ['lane']})
    expected.update({k: 'boolean' for k in ['paired_run']})
    expected.update({k: 'date'    for k in ['run_date']})
    expected.update({k: 'long'    for k in ['ubam_size']})
    url = elasticsearch_url('read_group', '_mapping', 'default')
    mapping = requests.get(url).json()['read_group']['mappings']['default']
    assert mapping['dynamic'] == 'false'
    properties = mapping['properties']
    assert len(expected) == len(properties)
    for name, type in expected.items():
        assert properties[name]['type'] == type


def test_authorization():
    withoutExpect = { # expected results from testWithout
        'OIDC_access_token':     'forbidden',
        'OIDC_CLAIM_expires_in': 'forbidden',
        'OIDC_CLAIM_email':      'forbidden',
        'OIDC_CLAIM_sub':        'ok',
        'OIDC_CLAIM_user_id':    'ok'
    }
    def mockHeaders(withoutExpect): # derive mock headers from withoutExpect
        result = {key : key for key in withoutExpect.keys()}
        oks = [key for key, value in withoutExpect.items() if value == 'ok']
        id = ' or '.join(oks)
        for ok in oks: result[ok] = id
        result['OIDC_CLAIM_expires_in'] = str(1234567890)
        return result
    headers = mockHeaders(withoutExpect)
    def getAuthStatus(headers):
        authUrl = clio_url('api', 'authorization')
        return requests.get(authUrl, headers=headers).status_code
    def without(header): # a copy of headers without header
        result = dict(headers)
        del result[header]
        return result
    def testWithout(header, status):
        assert getAuthStatus(without(header)) == requests.codes[status]
    for header, expect in withoutExpect.items(): testWithout(header, expect)


def new_uuid():
    return str(uuid.uuid4()).replace('-', '')

def read_group_metadata_location(location, upsertAssert):
    expected = {
        'flowcell_barcode': 'barcode2',
        'lane': 2,
        'library_name': 'library' + new_uuid(),
        'location' : location,
        'project': 'testProject'
    }
    upsert = {'project': expected['project']}
    upsertUrl = readgroup_v1_url('metadata',
                                 expected['flowcell_barcode'],
                                 expected['lane'],
                                 expected['library_name'],
                                 expected['location'])
    upsertAssert(requests.post(upsertUrl, json=upsert).json())
    query = {'library_name': expected['library_name']}
    url = readgroup_v1_url('query')
    return requests.post(url, json=query).json(), expected


def test_read_group_metadata():
    def assertRejected(response):
        assert response['rejection'] == 'The requested resource could not be found.'
    def assertEmpty(response):
        not response
    for location in ['GCP', 'OnPrem']:
        js, expected = read_group_metadata_location(location, assertEmpty)
        assert len(js) == 1
        assert js[0] == expected
    js, _ = read_group_metadata_location('Unknown', assertRejected)
    assertEmpty(js)


# I don't know how to derive the required fields.
#
def test_json_schema():
    def expected():
        schemas = { 'keyword': {"type": "string" },
                    'boolean': {"type": "boolean"},
                    'integer': {"type": "integer", "format": "int32"},
                    'long':    {"type": "integer", "format": "int64"},
                    'date':    {"type": "string" , "format": "date-time"} }
        r = requests.get(elasticsearch_url('read_group', '_mapping', 'default')).json()
        mapping = r['read_group']['mappings']['default']['properties']
        properties = { key: schemas[value['type']] for key, value in mapping.items() }
        return { 'type': 'object',
                 'required': [ 'flowcell_barcode', 'lane', 'library_name', 'location'],
                 'properties': properties }
    response = requests.get(clio_url('api', 'v1', 'readgroup', 'schema'))
    assert response.json() == expected()


def test_query_sample_project():
    project = 'testProject' + new_uuid()
    upsert = [{'flowcell_barcode': 'barcode2',
               'lane': 2,
               'library_name': 'library' + new_uuid(),
               'location': 'GCP',
               'project': project,
               'sample_alias': 'testSample' + new_uuid()} for _ in range(3)]
    sample = upsert[0]['sample_alias'] = upsert[1]['sample_alias']
    for up in upsert:
        upsertUrl = readgroup_v1_url('metadata',
                                     up['flowcell_barcode'],
                                     up['lane'],
                                     up['library_name'],
                                     up['location'])
        json = {k : up[k] for k in ['sample_alias', 'project']}
        assert requests.post(upsertUrl, json=json).ok
    queryUrl = readgroup_v1_url('query')
    projectJson = requests.post(queryUrl, json={'project': project}).json()
    assert len(projectJson) == 3
    for record in projectJson:
        assert record['project'] == project
    sampleJson = requests.post(queryUrl, json={'sample_alias': sample}).json()
    assert len(sampleJson) == 2
    for record in sampleJson:
        assert record['sample_alias'] == sample

def test_read_group_metadata_update():
    metadata = {
        'flowcell_barcode': 'barcode2',
        'lane': 2,
        'library_name': 'library' + new_uuid(),
        'location': 'GCP',
        'project': 'testProject' + new_uuid(),
        'sample_alias': 'sampleAlias1'
    }
    def querySample():
        r = requests.post(readgroup_v1_url('query'), json={'project': metadata['project']})
        return r.json()[0]['sample_alias']
    keys = ['flowcell_barcode', 'lane', 'library_name', 'location']
    url = readgroup_v1_url('metadata', *[metadata[k] for k in keys])
    upsert = {k : metadata[k] for k in ['sample_alias', 'project']}
    assert requests.post(url, json=upsert).ok
    assert querySample() == 'sampleAlias1'
    assert requests.post(url, json={'sample_alias': 'sampleAlias2'}).ok
    assert querySample() == 'sampleAlias2'


# Allow yellow Elasticsearch cluster health when running outside of
# the standard dockerized test.
#
if __name__ == '__main__':
    test_wait_for_clio(True)
    test_version()
    test_health(True)
    test_bad_method()
    test_bad_path()
    test_read_group_mapping()
    test_authorization()
    test_read_group_metadata()
    test_json_schema()
    test_query_sample_project()
    test_read_group_metadata_update()
    print('tests passed')
