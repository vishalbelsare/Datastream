# -*- coding: utf-8 -*-
"""
Created on Tue Jan  1 19:51:02 2019

@author: Vidya Dinesh
"""
import requests
import json
import pandas as pd
import datetime
import pytz

import DS_Requests as DSReq

#--------------------------------------------------------------------------------------
class Datastream:
    """Datastream helps to retrieve data from DSWS web rest service"""
    url = "http://product.datastream.com/DSWSClient/V1/DSService.svc/rest/"
    username = ""
    password = ""
    token = None
    dataSource = None

#--------Constructor ---------------------------  
    def __init__(self, username, password, dataSource=None):
        self.username = username
        self.password = password
        self.dataSource = dataSource
        self.token = self._get_token()
#-------------------------------------------------------  
#------------------------------------------------------- 
    def post_user_request(self, tickers, fields=None, start='', end='', freq='', kind=1):
        """ This function helps to form requests for get_bundle_data. 
            Each request is converted to JSON format."""
            
        if fields == None:
            fields=[]
                         
        index = tickers.rfind('|')
        try:
            if index == -1:
                instrument = DSReq.Instrument(tickers, None)
            else:
                #Get all the properties of the instrument
                props = []
                if tickers[index+1:].rfind(',') != -1:
                    propList = tickers[index+1:].split(',')
                    for eachProp in propList:
                        props.append(DSReq.IProperties(eachProp, True))
                else:
                    props.append(DSReq.IProperties(tickers[index+1:], True))
                    #Get the no of instruments given in the request
                    instList =  tickers[0:index].split(',')
                    if len(instList) > 40:
                        raise Exception('Too many instruments in single request')
                    else:
                        instrument = DSReq.Instrument(tickers[0:index], props)
                        
            datypes=[]
            if len(fields) > 0:
                if len(fields) > 20:
                    raise Exception('Too mant datatypes in single request')
                else:
                    for eachDtype in fields:
                        datypes.append(DSReq.DataType(eachDtype))
            else:
                datypes.append(DSReq.DataType(fields))
                        
            date = DSReq.Date(start, freq, end, kind)
            request = {"Instrument":instrument,"DataTypes":datypes,"Date":date}
            return request
        except Exception as err:
            print(err)
            return None
            
    def get_data(self, tickers, fields=None, start='', end='', freq='', kind=1):
        """This Function processes a single JSON format request to provide
           data response from DSWS web in the form of python Dataframe"""
        getData_url = self.url + "GetData"
        raw_dataRequest = ""
        json_dataRequest = ""
        json_Response = ""
        
        if fields == None:
            fields = []
        
        try:
            req = self.post_user_request(tickers, fields, start, end, freq, kind)
            datarequest = DSReq.DataRequest()
            if (self.token == None):
                raise Exception("Invalid Token Value")
            else:
                raw_dataRequest = datarequest.get_Request(req, self.dataSource, 
                                                      self.token)
                #print(raw_dataRequest)
            if (raw_dataRequest != ""):
                json_dataRequest = self._json_Request(raw_dataRequest)
                #Post the requests to get response in json format
                json_Response = requests.post(getData_url, json=json_dataRequest).json()
                #print(json_Response)
                #format the JSON response into readable table
                response_dataframe = self._format_Response(json_Response['DataResponse'])
                return response_dataframe
        except json.JSONDecodeError:
            print("JSON decoder error while get_data request")
            return None
        except:
            print("get_data : Unexpected error")
            return None
    
    def get_bundle_data(self, bundleRequest=None):
        """This Function processes a multiple JSON format data requests to provide
           data response from DSWS web in the form of python Dataframe.
           Use post_user_request to form each JSON data request and append to a List
           to pass the bundleRequset."""
        getDataBundle_url = self.url + "GetDataBundle"
        raw_dataRequest = ""
        json_dataRequest = ""
        json_Response = ""
        
        if bundleRequest == None:
            bundleRequest = []
        
        try:
            datarequest = DSReq.DataRequest()
            if (self.token == None):
                raise Exception("Invalid Token Value")
            else:
                raw_dataRequest = datarequest.get_bundle_Request(bundleRequest, 
                                                             self.dataSource, 
                                                             self.token)
            #print(raw_dataRequest)
            if (raw_dataRequest != ""):
                json_dataRequest = self._json_Request(raw_dataRequest)
                #Post the requests to get response in json format
                json_Response = requests.post(getDataBundle_url, json=json_dataRequest).json()
                #print(json_Response)
                response_dataframe = self._format_bundle_response(json_Response)
                return response_dataframe
        except json.JSONDecodeError:
            print("JSON decoder error while get_data request")
            return None
        except:
            print("get_bundle_data : Unexpected error")
            return None
    
#------------------------------------------------------- 
#-------------------------------------------------------             
#-------Helper Functions---------------------------------------------------
    def _get_token(self):
        token_url = self.url + "GetToken"
        try:
            tokenReq = DSReq.TokenRequest(self.username, self.password, self.dataSource)
            raw_tokenReq = tokenReq.get_TokenRequest()
            json_tokenReq = self._json_Request(raw_tokenReq)
            #Post the token request to get response in json format
            json_Response = requests.post(token_url, json=json_tokenReq).json()
            return json_Response["TokenValue"]
        except json.JSONDecodeError:
            print("JSON decoder error while posting Token request")
            return None
        except:
            print("Token Request : Unexpected error")
            return None
            
    def _json_Request(self, raw_text):
        #convert the dictionary (raw text) to json text first
        jsonText = json.dumps(raw_text)
        byteTemp = bytes(jsonText,'utf-8')
        byteTemp = jsonText.encode('utf-8')
        #convert the json Text to json formatted Request
        jsonRequest = json.loads(byteTemp)
        return jsonRequest

    def _get_Date(self, jsonDate):
        d = jsonDate[6:-7]
        d = float(d)
        ndate = datetime.datetime(1970,1,1) + datetime.timedelta(seconds=float(d)/1000)
        utcdate = pytz.UTC.fromutc(ndate).strftime('%Y-%m-%d')
        return utcdate
    
    def _get_DatatypeValues(self, jsonDTValues):
        df = pd.DataFrame()
        multiIndex = False
        valDict = {"Instrument":[],"Datatype":[],"Value":[]}
       
        for item in jsonDTValues: 
           datatype = item['DataType']
           for i in item['SymbolValues']:
               instrument = i['Symbol']
               valDict["Datatype"].append(datatype)
               valDict["Instrument"].append(instrument)
               values = i['Value']
               valType = i['Type']
               colNames = (instrument,datatype)
               df[colNames] = None
               
               """Handling all possible types of data as per DSSymbolResponseValueType"""
               if valType in [7, 8, 10, 11, 12, 13, 14, 15, 16]:
                   """These value types return an array
                   The array can be of double, int, string or Object"""
                   rowCount = df.shape[0]
                   valLen = len(values)
                   """If no of Values is < rowcount, append None to values"""
                   if rowCount > valLen:
                       for i in range(rowCount - valLen):
                            values.append(None)
                   """ Check if the array of Object is JSON dates and convert""" 
                   for x in range(0, len(values)):
                       values[x] = self._get_Date(values[x]) if str(values[x]).find('/Date(') != -1 else values[x] 
                   df[colNames] = values
                   multiIndex = True
               elif valType in [1, 2, 3, 5, 6]:
                   """These value types return single value"""
                   valDict["Value"].append(values)
                   multiIndex = False
               else:
                   if valType == 4:
                       """value type 4 return single JSON date value, 
                       which needs conversion"""
                       values = self._get_Date(values)
                       valDict["Value"].append(values)
                       multiIndex = False
                   elif valType == 9:
                       """value type 9 return array of JSON date values, 
                       which needs conversion"""
                       multiIndex = True
                       date_array = []
                       for eachVal in values:
                           date_array.append(self._get_Date(eachVal))
                           df[colNames] = values
                   else:
                       if valType == 0:
                           """Error Returned"""
                           multiIndex = False
                           valDict["Value"].append(values)
                           
               if multiIndex:
                   df.columns = pd.MultiIndex.from_tuples(df.columns, names=['Instrument', 'Field'])
                       
        if not multiIndex:
            indexLen = range(len(valDict['Instrument']))
            newdf = pd.DataFrame(data=valDict,columns=["Instrument", "Datatype", "Value"],
                                 index=indexLen)
            return newdf
        return df 
            
    def _format_Response(self, response_json):
        # If dates is not available, the request is not constructed correctly
        response_json = dict(response_json)
        if 'Dates' in response_json.keys():
            dates_converted = []
            if response_json['Dates'] != None:
                dates = response_json['Dates']
                for d in dates:
                    dates_converted.append(self._get_Date(d))
        else:
            return 'Error - please check instruments and parameters (time series or static)'
        
        # Loop through the values in the response
        dataframe = self._get_DatatypeValues(response_json['DataTypeValues'])
        if (len(dates_converted) == len(dataframe.index)):
            if (len(dates_converted) > 1):
                dataframe.insert(loc = 0, column = 'Dates', value = dates_converted)
        elif (len(dates_converted) == 1):
            dataframe['Dates'] = dates_converted[0]
            
        return dataframe

    def _format_bundle_response(self,response_json):
       formattedResp = []
       for eachDataResponse in response_json['DataResponses']:
           df = self._format_Response(eachDataResponse)
           formattedResp.append(df)      
           
       return formattedResp
#--------------------------------------------------------------------------------------
    
