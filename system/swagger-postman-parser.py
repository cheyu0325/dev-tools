import os
import shutil
import time
import yaml
import json


# --------------------------------------------------
# BaseUrl, mapping the yaml file name with base url
# --------------------------------------------------
class BaseUrl:
    def getBaseUrl(self, _yamlName):
        _urlPathsMapper = {
            # platform system yaml mappings
            'base-url': "{{base_url}}",
            'admin-base-url': "{{admin_base_url}}",
            'activity-v1.yaml': "{{activity_url}}",
            'engagementWidget-v1.yaml': "{{ew_url}}",
            'member-v1.yaml': "{{member_url}}",
            'admin-v1.yaml': "{{admin_url}}",
            'fanfeed-old-v1.yaml': "{{feed_old_url}}",
            'baboon-admin-v1.yaml': "{{baboon_admin_url}}",
            'fanfeed-v1.yaml': "{{fanfeed_url}}",
            'misc-v1.yaml': "{{misc_url}}",
            'baboon-v1.yaml': "{{baboon_url}}",
            'feed-v1.yaml': "{{feed_url}}",
            'multimedia-v1.yaml': "{{multimedia_url}}",
            'billing-v1.yaml': "{{billing_url}}",
            'forum-v1.yaml': "{{forum_url}}",
            'post-v1.yaml': "{{}}",
            'boost-v1.yaml': "{{boost_url}}",
            'golive-v1.yaml': "{{golive_url}}",
            'rms-v1.yaml': "{{rms_url}}",
            'device-v1.yaml': "{{device_url}}",
            'inbox-v1.yaml': "{{inbox_url}}",

            # legacy system yaml mappings
            'membership-v1.yaml': "{{membership_url}}"
        }
        return _urlPathsMapper.get(_yamlName)

    def isAdminApi(self, _yamlName):
        _baseUrl = self.getBaseUrl(_yamlName)
        if 'admin' in _baseUrl:
            return True
        return False


# --------------------------------------------------
# ParameterHandler, handle the parameters process
# --------------------------------------------------
class ParameterHandler:
    def praseHashProp(self, params, swaggerTemplate):
        hashProp = {'key': None, 'value': None}
        if params.get('$ref') is not None and '#/parameters/' in params.get('$ref'):
            propKey = params['$ref'].replace('#/parameters/', '')
            hashProp['key'] = propKey
            hashProp['value'] = swaggerTemplate.getParameter(propKey)
        elif params.get('$ref') is not None and '#/definitions/' in params.get('$ref'):
            propKey = params['$ref'].replace('#/definitions/', '')
            hashProp['key'] = propKey
            hashProp['value'] = swaggerTemplate.getDefinition(propKey)
        else:
            hashProp['value'] = params
        return hashProp

    def getRefIn(self, param, swaggerTemplate):
        prop = self.praseHashProp(param, swaggerTemplate)
        return prop.get('value')['in']

    def getNotRefIn(self, param):
        return param.get('in')

    def isHeadAuthorizationParam(self, param, swaggerTemplate):
        if '$ref' not in param and self.getNotRefIn(param) == 'header':
            return True
        return False

    def isPathParam(self, param, swaggerTemplate):
        if '$ref' in param and self.getRefIn(param, swaggerTemplate) == 'path':
            return True
        elif self.getNotRefIn(param) == 'path':
            return True
        return False

    def isQueryParam(self, param, swaggerTemplate):
        if '$ref' in param and self.getRefIn(param, swaggerTemplate) == 'query':
            return True
        elif self.getNotRefIn(param) == 'query':
            return True
        return False

    def isBodyData(self, param, swaggerTemplate):
        if '$ref' not in param and self.getNotRefIn(param) == 'body':
            return True
        return False

    def isFormData(self, param, swaggerTemplate):
        if '$ref' not in param and self.getNotRefIn(param) == 'formData':
            return True
        return False

    def setPathParam(self, _url, _param):
        if _url.get('variable') is None:
            _url['variable'] = []

        pathParam = {"key": _param.get('name'), "value": ""}
        if _param.get('default') is not None:
            pathParam['value'] = str(_param['default'])
        _url['variable'].append(pathParam)

    def setQueryParam(self, _url, _param):
        if _url.get('query') is None:
            _url['query'] = []

        pathParam = {"key": _param.get('name'), "value": ""}
        if _param.get('default') is not None:
            pathParam['value'] = str(_param['default'])
        _url['query'].append(pathParam)

    # def setBodyData(self, _body, _swaggerTemplate, param, paramKey):
    def setBodyData(self, _body, _swaggerTemplate, param):
        def _praseObject(_object):
            data = {}
            _props = _object.get('properties')

            # check the necessary parameter 'properties'
            if _props is None:
                return data

            # prase the necessary properties for request body
            for prop in _props:
                if '$ref' in _props.get(prop):
                    _prop = ParameterHandler().praseHashProp(_props.get(prop), _swaggerTemplate)
                    data[prop] = _praseRef(_prop.get('value'))
                elif _props.get(prop).get('type') == 'object':
                    data[prop] = _praseObject(_props.get(prop))
                elif _props.get(prop).get('type') == 'array':
                    data[prop] = _praseArray(_props.get(prop).get('example'))
                else:
                    _value = _praseValue(_props.get(prop))
                    data[prop] = _value
            return data

        def _praseRef(_prop):
            if '$ref' in _prop:
                _prop = ParameterHandler().praseHashProp(_prop, _swaggerTemplate)
                return _praseRef(_prop.get('value'))
            elif _prop.get('type') == 'object':
                return _praseObject(_prop)
            elif _prop.get('type') == 'array':
                return _praseArray(_prop.get('example'))
            else:
                return _praseValue(_prop)

        def _praseValue(_prop):
            if _prop.get('default') is None:
                return _prop.get('type')
            return _prop.get('default')

        def _praseArray(_props):
            data = []
            if _props is not None:
                for prop in _props:
                    data.append(prop)
            return data

        try:
            if param.get('value').get('type') == 'object':
                _body['raw'] = _praseObject(param.get('value'))
            elif param.get('value').get('type') == 'array':
                _body['raw'] = _praseArray(param.get('value').get('example'))
            else:
                _body['raw'] = _praseValue(param.get('value'))
        except Exception as e:
            print("Error to prase bodyData, " + param)
            print(e)

    def setFormData(self, _body, _param):
        prop = {'key': _param.get('name'), 'value': _param.get(
            'type'), 'type': 'text'}
        _body.get('urlencoded').append(prop)


# --------------------------------------------------
# ApiItem class, handle the api json contents
# --------------------------------------------------
class ApiItem:
    def __init__(self, category, baseUrl, urlPath, name, auth, method, header, params, swaggerTemplate):
        self.content = {}
        self.isFormUrlEncoded = False
        self.setName(name)
        self.setRequest()
        self.setMethod(method)
        self.setHeader(header)
        self.setContents(baseUrl, urlPath, params, swaggerTemplate)
        self.setResponse()

    def setName(self, _name):
        self.content.setdefault('name', _name)

    def setRequest(self):
        self.content.setdefault('request', {})

    def setMethod(self, _method):
        self.content.get('request').setdefault('method', _method.upper())

    def setHeader(self, _header):
        for _content in _header:
            if _content.get('key') == 'Content-Type' and _content.get('value') == 'application/x-www-form-urlencoded':
                self.isFormUrlEncoded = True
        self.content.get('request').setdefault('header', _header)

    def setContents(self, baseUrl, urlPath, params, swaggerTemplate):
        def _parseBodyType():
            _body = None
            if self.isFormUrlEncoded == True:
                _body = {'mode': 'urlencoded', 'urlencoded': []}
            else:
                _body = {'mode': 'raw', 'raw': ''}
            return _body

        def _parseHost(_url):
            return [_url]

        def _parseUrlParamFormat(_p):
            if _p.startswith('{') and _p.endswith('}'):
                return _p.replace('{', ':').replace('}', '')
            return _p

        def _parsePath(_url):
            _pathParams = _url.split("/")
            if _pathParams[0] == '':
                del _pathParams[0]
            _i = 0
            while(_i < len(_pathParams)):
                _pathParams[_i] = _parseUrlParamFormat(_pathParams[_i])
                _i = _i + 1
            return _pathParams

        def _parseRaw(_base, _url):
            _pathParams = _parsePath(_url)
            _raw = '' + _base
            for _param in _pathParams:
                _raw = _raw + '/' + _param
            return _raw

        def _parseParams(_url, _body, _params, _swaggerTemplate):
            for param in _params:
                try:
                    if ParameterHandler().isHeadAuthorizationParam(param, _swaggerTemplate):
                        continue
                    if ParameterHandler().isPathParam(param, _swaggerTemplate):
                        _prop = ParameterHandler().praseHashProp(param, _swaggerTemplate)
                        ParameterHandler().setPathParam(_url, _prop.get('value'))
                        continue
                    if ParameterHandler().isQueryParam(param, _swaggerTemplate):
                        _prop = ParameterHandler().praseHashProp(param, _swaggerTemplate)
                        ParameterHandler().setQueryParam(_url, _prop.get('value'))
                        continue
                    if ParameterHandler().isBodyData(param, _swaggerTemplate):
                        _prop = ParameterHandler().praseHashProp(
                            param['schema'], _swaggerTemplate)
                        ParameterHandler().setBodyData(_body, _swaggerTemplate, _prop)
                        continue
                    if ParameterHandler().isFormData(param, _swaggerTemplate):
                        ParameterHandler().setFormData(_body, param)
                        continue
                    print('Error: Unknow Parameter %s !!!'.replace('%s', param))
                except Exception as e:
                    print("Error to prase parameters, param:" + param)
                    print(e)

        _url = {}
        _url.setdefault('raw', _parseRaw(baseUrl, urlPath))
        _url.setdefault('host', _parseHost(baseUrl))
        _url.setdefault('path', _parsePath(urlPath))

        _body = _parseBodyType()
        if params is not None:
            _parseParams(_url, _body, params, swaggerTemplate)

        self.content.get('request').setdefault('url', _url)
        self.content.get('request').setdefault('body', _body)

    def setResponse(self):
        self.content.setdefault('response', [])


# --------------------------------------------------------------------------------
# SwaggerTemplate, generate the api info from yaml file to postman json pattern
# --------------------------------------------------------------------------------
class SwaggerTemplate:
    def __init__(self, yamlName, title, paths, parameters, definitions):
        self.content = {}
        self.categories = {}
        self.yamlName = yamlName
        self.title = title
        self.setInfo(title)
        self.isAdminApi = BaseUrl().isAdminApi(self.yamlName)
        self.basePath = BaseUrl().getBaseUrl(self.yamlName)
        self.parameters = parameters
        self.definitions = definitions
        for pathKey in paths:
            self.parsePaths(pathKey, paths.get(pathKey))

    def setInfo(self, title):
        _info = {'_postman_id': '', 'name': title,
                 'schema': 'https://schema.getpostman.com/json/collection/v2.1.0/collection.json'}
        self.content['info'] = _info

    def parsePaths(self, _pathKey, pathItem):
        for httpMethod in pathItem:
            try:
                _category = pathItem.get(httpMethod)['tags'][0]
                _baseUrl = self.basePath
                _urlPath = _pathKey
                _name = pathItem.get(httpMethod).get('description')
                _auth = self.getAuth(self.yamlName)
                _method = httpMethod
                _header = self.getHeader(
                    pathItem.get(httpMethod).get('consumes'))
                _params = pathItem.get(httpMethod).get('parameters')
                apiItem = ApiItem(_category, _baseUrl, _urlPath,
                                  _name, _auth, _method, _header, _params, self)
                self.addApiItem(_category, apiItem)
            except Exception as e:
                print("Error to prase pathItem, path:" +
                      _pathKey + ", httpMethod:" + httpMethod)
                print(e)

    def getAuth(self, _yamlNaml):
        return {}

    def getHeader(self, _consumes):
        _header = []
        _token = {'key': 'Authorization'}
        _contentType = {'key': 'Content-Type'}

        # handle the content-type of header
        if self.isAdminApi:
            _token.setdefault('value', 'Bearer {{admin_token}}')
        else:
            _token.setdefault('value', 'Bearer {{token}}')

        # handle the content-type of header
        if _consumes is not None and 'application/x-www-form-urlencoded' in _consumes:
            _contentType.setdefault(
                'value', 'application/x-www-form-urlencoded')
        else:
            _contentType.setdefault('value', 'application/json')

        # arrange the header info
        _header.append(_token)
        _header.append(_contentType)
        return _header

    def getParameter(self, key):
        return self.parameters.get(key)

    def getDefinition(self, key):
        return self.definitions.get(key)

    def addApiItem(self, _category, _apiItem):
        if self.categories.get(_category) is None:
            self.categories[_category] = {'name': _category, 'item': []}
        self.categories.get(_category).get('item').append(_apiItem.content)

    def exportToPostmanJson(self):
        self.content['item'] = []
        for category in self.categories:
            self.content['item'].append(self.categories.get(category))
        return json.dumps(self.content, indent=2, sort_keys=True)


# --------------------------------------------------------------------------------
# Main Flow !!!!!
# --------------------------------------------------------------------------------
root = os.path.abspath(os.path.dirname(__file__))
target = root + '/postman/collection'
docsPath = root + '/docs'
if os.path.exists(target):
    shutil.rmtree(target)
    os.mkdir(target)
else:
    os.mkdir(target)

print(">>>> start parsing process")

files = os.listdir(docsPath)
for file in files:
    if file == '.DS_Store':
        continue

    print(">>>> processing : " + file)
    try:
        with open(docsPath + '/' + file, "r") as stream:
            data = yaml.load(stream)
            stream.close()

        swagger = SwaggerTemplate(file, data.get('info').get('title'), data.get(
            'paths'), data.get('parameters'), data.get('definitions'))
        jsonFileName = target + '/' + swagger.title + '.postman_collection.json'
        _api = open(jsonFileName, "w")
        _api.write(swagger.exportToPostmanJson())
        _api.flush()
        _api.close()
    except Exception as e:
        print("Error file:" + file)
        print(e)

print(">>>> end parsing process")
