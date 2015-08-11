#!/usr/bin/env python

class JSONUtil:
    def __init__(self):
        pass

    @staticmethod
    def replace_values_at_path(jsonInput, jsonPath, findUri, replaceJson, removeElements):
        findObjs = JSONUtil.extract_objects_from_path(jsonInput, jsonPath)
        for findObj in findObjs:
            if findObj["uri"] == findUri:
                for elem_name in removeElements:
                    if elem_name in findObj:
                        del findObj[elem_name]

                for elem_name in replaceJson:
                    findObj[elem_name] = replaceJson[elem_name]

        return jsonInput

    @staticmethod
    def extract_objects_from_path(jsonInput, jsonPath):
        return list(JSONUtil.__extract_from_path(jsonInput, jsonPath, False))

    @staticmethod
    def extract_values_from_path(jsonInput, jsonPath):
        return list(JSONUtil.__extract_from_path(jsonInput, jsonPath, True))


    @staticmethod
    def __extract_from_path(jsonInput, jsonPath, only_uri):
        path_elems = jsonPath.split(".")
        start = JSONUtil.to_list(jsonInput)

        found = True
        for path_elem in path_elems:
            start = JSONUtil.__extract_elements(start, path_elem)
            if len(start) == 0:
                found = False
                break

        if found:
            if isinstance(start, list):
                for elem in start:
                    if "uri" in elem:
                        if only_uri is True:
                            yield elem["uri"]
                        else:
                            yield elem
            elif "uri" in start:
                if only_uri is True:
                    yield start["uri"]
                else:
                    yield start

    @staticmethod
    def __extract_elements(array, elem_name):
        result = []
        for elem in array:
            if elem_name in elem:
                elem_part = elem[elem_name]
                if isinstance(elem_part, list):
                    result.extend(elem_part)
                else:
                    result.append(elem_part)
        return result

    @staticmethod
    def to_list(some_object):
        if not isinstance(some_object, list):
            arr = list()
            arr.append(some_object)
            return arr
        return some_object

if __name__ == "__main__":
    import json
    jsonStr1 = '{"@context":"https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/2.0/karma/context.json","a":"Patent","assignee":{"startDate":"1995-03-17","a":"schema:Role","assignee":{"address":{"schema:postalCode":"43041","addressLocality":"MARYSVILLE","addressRegion":"OHIO","a":"PostalAddress","schema:streetAddress":"14111 SCOTTSLAWN ROAD","uri":"http://dig.isi.edu/patents/data/address/_14111scottslawnroad_marysville_ohio_43041_"},"a":"PersonOrOrganization","name":"SCOTTS COMPANY, THE","uri":"http://dig.isi.edu/patents/data/organization/scottscompanythe"}},"uri":"http://dig.isi.edu/patents/data/patent/0006279","identifier":{"a":"Identifier","name":"0006279","hasType":"http://dig.isi.edu/patents/data/thesaurus/identifier/patentid"}}'
    jsonObj1 = json.loads(jsonStr1)
    print list(JSONUtil.extract_values_from_path(jsonObj1, "assignee.assignee"))

    print "\n******************************"
    jsonStr2 = '{"@context":"https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/2.0/karma/context.json","a":"Patent","assignee":{"startDate":"1995-03-17","a":"schema:Role","assignee":[{"address":{"schema:postalCode":"43041","addressLocality":"MARYSVILLE","addressRegion":"OHIO","a":"PostalAddress","schema:streetAddress":"14111 SCOTTSLAWN ROAD","uri":"http://dig.isi.edu/patents/data/address/_14111scottslawnroad_marysville_ohio_43041_"},"a":"PersonOrOrganization","name":"SCOTTS COMPANY, THE","uri":"http://dig.isi.edu/patents/data/organization/scottscompanythe"},{"address":{"schema:postalCode":"43041","addressLocality":"MARYSVILLE","addressRegion":"OHIO","a":"PostalAddress","schema:streetAddress":"14111 SCOTTSLAWN ROAD","uri":"http://dig.isi.edu/patents/data/address/_14111scottslawnroad_marysville_ohio_43041_"},"a":"PersonOrOrganization","name":"SCOTTS COMPANY, THE","uri":"http://dig.isi.edu/patents/data/organization/scottscompanythe2"}]},"uri":"http://dig.isi.edu/patents/data/patent/0006279","identifier":{"a":"Identifier","name":"0006279","hasType":"http://dig.isi.edu/patents/data/thesaurus/identifier/patentid"}}'
    jsonObj2 = json.loads(jsonStr2)
    print list(JSONUtil.extract_values_from_path(jsonObj2, "assignee.assignee"))

    print "\n******************************"
    jsonStr3 = '{"@context":"https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/2.0/karma/context.json","a":"Patent","assignee":[{"startDate":"1995-03-17","a":"schema:Role","assignee":{"address":{"schema:postalCode":"43041","addressLocality":"MARYSVILLE","addressRegion":"OHIO","a":"PostalAddress","schema:streetAddress":"14111 SCOTTSLAWN ROAD","uri":"http://dig.isi.edu/patents/data/address/_14111scottslawnroad_marysville_ohio_43041_"},"a":"PersonOrOrganization","name":"SCOTTS COMPANY, THE","uri":"http://dig.isi.edu/patents/data/organization/scottscompanythe"}},{"startDate":"1995-03-17","a":"schema:Role","assignee":{"address":{"schema:postalCode":"43041","addressLocality":"MARYSVILLE","addressRegion":"OHIO","a":"PostalAddress","schema:streetAddress":"14111 SCOTTSLAWN ROAD","uri":"http://dig.isi.edu/patents/data/address/_14111scottslawnroad_marysville_ohio_43041_"},"a":"PersonOrOrganization","name":"SCOTTS COMPANY, THE","uri":"http://dig.isi.edu/patents/data/organization/scottscompanythe2"}}],"uri":"http://dig.isi.edu/patents/data/patent/0006279","identifier":{"a":"Identifier","name":"0006279","hasType":"http://dig.isi.edu/patents/data/thesaurus/identifier/patentid"}}'
    jsonObj3 = json.loads(jsonStr3)
    print list(JSONUtil.extract_values_from_path(jsonObj3, "assignee.assignee"))