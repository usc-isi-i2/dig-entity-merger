#!/usr/bin/env python

class JSONUtil:
    def __init__(self):
        pass

    @staticmethod
    def test_is_basestring_uri_match(strDictOrListValue, findUri):
        return isinstance(strDictOrListValue, basestring) and strDictOrListValue == findUri

    @staticmethod
    def test_is_dict_uri_match(strDictOrListValue, findUri):
        return isinstance(strDictOrListValue, dict) and (("uri" in strDictOrListValue and strDictOrListValue["uri"] == findUri )or ("@id" in strDictOrListValue and strDictOrListValue["@id"] == findUri))        

    @staticmethod
    def is_reserved_json_ld_property(prop):
        return (prop[0] == "@"  or prop == "a" or prop == "uri")
        
    @staticmethod
    def frame_include_only_values(jsonToEdit, frame):
        for elem_name in jsonToEdit.keys():
            if not elem_name in frame and not JSONUtil.is_reserved_json_ld_property(elem_name):
                del jsonToEdit[elem_name]
        return jsonToEdit

    @staticmethod
    def replace_values(jsonToEdit, replaceJson, removeElements) :
        for elem_name in removeElements:
            if elem_name in jsonToEdit:
                del jsonToEdit[elem_name]
        for elem_name in replaceJson:
            jsonToEdit[elem_name] = replaceJson[elem_name]
        return jsonToEdit

    @staticmethod
    def replace_values_at_path_list(jsonInput, jsonPathList, findUri, replaceJson, removeElements):    
        if(len(jsonPathList) == 0):
            return         
        elif(len(jsonPathList) == 1):
            strDictOrListValue = jsonInput[jsonPathList[0]]

            if isinstance(strDictOrListValue, list):
                newList = list()
                listValue = strDictOrListValue
                for strDictOrListElement in listValue :
                    if JSONUtil.test_is_basestring_uri_match(strDictOrListElement, findUri):
                        newList.append(replaceJson)
                    elif JSONUtil.test_is_dict_uri_match(strDictOrListElement, findUri):
                        newList.append(JSONUtil.replace_values(strDictOrListElement, replaceJson, removeElements))
                    else:
                        newList.append(strDictOrListElement)
                jsonInput[jsonPathList[0]] = newList
            elif JSONUtil.test_is_basestring_uri_match(strDictOrListValue, findUri) :
                jsonInput[jsonPathList[0]] = replaceJson
            elif JSONUtil.test_is_dict_uri_match(strDictOrListValue, findUri):
                jsonInput[jsonPathList[0]] = JSONUtil.replace_values(strDictOrListValue, replaceJson, removeElements)
            
        else :
            strDictOrListValue = jsonInput[jsonPathList[0]]
            if isinstance(strDictOrListValue, list):
                listValue = strDictOrListValue
                for strDictOrListElement in listValue :
                    if(isinstance(strDictOrListElement, basestring)):
                        continue
                    elif(isinstance(strDictOrListElement, dict)):
                        dictElement = strDictOrListElement
                        JSONUtil.replace_values_at_path_list(dictElement, jsonPathList[1:], findUri, replaceJson, removeElements)
                    else:
                        continue
            elif isinstance(strDictOrListValue, dict):
                dictValue = strDictOrListValue
                JSONUtil.replace_values_at_path_list(dictValue, jsonPathList[1:], findUri, replaceJson, removeElements)
            
        return jsonInput


    @staticmethod
    def replace_values_at_path(jsonInput, jsonPath, findUri, replaceJson, removeElements):
        splitJsonPath = jsonPath.split(".")
        if(splitJsonPath[0]== "$"):
            splitJsonPath = splitJsonPath[1:]
        return JSONUtil.replace_values_at_path_list(jsonInput, splitJsonPath, findUri, replaceJson, removeElements)

  #  @staticmethod
  #  def replace_values_at_path(jsonInput, jsonPath, findUri, replaceJson, removeElements):
  #      findObjs = JSONUtil.extract_objects_from_path(jsonInput, jsonPath)
  #      for findObj in findObjs:
  #          if findObj["uri"] == findUri:
  #              for elem_name in removeElements:
  #                  if elem_name in findObj:
  #                      del findObj[elem_name]

   #             for elem_name in replaceJson:
   #                 findObj[elem_name] = replaceJson[elem_name]

 #       return jsonInput

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
                    if only_uri is True:
                        if "uri" in elem:
                            yield elem["uri"]
                        elif isinstance(elem, unicode) or isinstance(elem, str): 
                            yield elem
                    else:
                        yield elem
            elif only_uri is True and "uri" in start:
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

    print "\n******************************"
    jsonStr4 = '{"a": "Offer", "uri": "http://test.com/offer/a", "ad": ["http://test.com/ad/a"], "seller": "http://test.com/seller/a" }'
    jsonObj4 = json.loads(jsonStr4)
    print list(JSONUtil.extract_values_from_path(jsonObj4, "seller"))

    print "\n******************************"
    jsonStr5 = '{"uri": "http://test.com/seller/a", "a": "Seller", "label": "Acme"}'
    jsonObj5 = json.loads(jsonStr5)
    print JSONUtil.replace_values_at_path(jsonObj4, "seller", "http://test.com/seller/a", jsonObj5,[])