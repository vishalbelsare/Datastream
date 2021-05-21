
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
import traceback
import platform
import configparser
import atexit
import re


from .DS_Requests import TokenRequest, Instrument, Properties, DataRequest, DataType, Date

#--------------------------------------------------------------------------------------
class Datastream:
    """Datastream helps to retrieve data from DSWS web rest service"""
    url = "https://product.datastream.com/DSWSClient/V1/DSService.svc/rest/"
    username = ""
    password = ""
    tokenResp = None
    dataSource = None
    _proxy = None
    _sslCer = None
    appID = "PythonLib 1.0.9"
    certfile = None
   
    
#--------Constructor ---------------------------  
    def __init__(self, username, password, config=None, dataSource=None, proxy=None, sslCer= None):
        if (config):
            parser = configparser.ConfigParser()
            parser.read(config)
            self.url = None if parser.get('url','path').strip() == '' else parser.get('url', 'path').strip()
            self.url = self.url.lower()
            if self.url:
                if re.match("^http:", self.url):
                    self.url = self.url.replace('http:', 'https:', 1) 
            self.url = self.url +'/DSWSClient/V1/DSService.svc/rest/'
        if proxy:
            self._proxy = {'http':proxy, 'https':proxy}
        if sslCer:
            self._sslCer = sslCer
        self.username = username
        self.password = password
        self.dataSource = dataSource
        self.tokenResp = self._get_token()
        
#-------------------------------------------------------  
#------------------------------------------------------- 
    def post_user_request(self, tickers, fields=None, start='', end='', freq='', kind=1, retName=False):
        """ This function helps to form requests for get_bundle_data. 
            Each request is converted to JSON format.
            
            Args:
               tickers: string, Dataypes 
               fields: List, default None
               start: string, default ''
               end : string, default ''
               freq : string, default '', By deafult DSWS treats as Daily freq
               kind: int, default 1, indicates Timeseries as output

          Returns:
                  Dictionary"""

            
        if fields == None:
            fields=[]
        
        index = tickers.rfind('|')
       
        if (retName):
            if index == -1:
                tickers = tickers + '|R'
            else:
                tickers = tickers + ',R'
         
        index = tickers.rfind('|')
        try:
            if index == -1:
                instrument = Instrument(tickers, None)
            else:
                #Get all the properties of the instrument
                props = []  
                if tickers[index+1:].rfind(',') != -1:
                    propList = tickers[index+1:].split(',')
                    for eachProp in propList:
                        props.append(Properties(eachProp, True))
                else:
                    props.append(Properties(tickers[index+1:], True))
#                    #Get the no of instruments given in the request 
                     #Commenting as this count is not needed
#                    instList =  tickers[0:index].split(',')
#                    if len(instList) > 40:
#                        raise Exception('Too many instruments in single request')
#                    else:
                    instrument = Instrument(tickers[0:index], props)
                        
            datypes=[]
            prop = [{'Key':'ReturnName', 'Value':True}] if retName else None
            
            if len(fields) > 0:
                #Commenting as this count and validation is not needed
#                if len(fields) > 20:
#                    raise Exception('Too mant datatypes in single request')
#                else:
                for eachDtype in fields:
                    datypes.append(DataType(eachDtype, prop))
            else:
                datypes.append(DataType(fields, prop))
                        
            date = Date(start, freq, end, kind)
            request = {"Instrument":instrument,"DataTypes":datypes,"Date":date}
            return request
        except Exception:
            print("post_user_request : Exception Occured")
            print(traceback.sys.exc_info())
            print(traceback.print_exc(limit=5))
            return None
            
    def get_data(self, tickers, fields=None, start='', end='', freq='', kind=1, retName=False):
        """This Function processes a single JSON format request to provide
           data response from DSWS web in the form of python Dataframe
           
           Args:
               tickers: string, Dataypes 
               fields: List, default None
               start: string, default ''
               end : string, default ''
               freq : string, default '', By deafult DSWS treats as Daily freq
               kind: int, default 1, indicates Timeseries as output
               retName: bool, default False, to be set to True if the Instrument
                           names and Datatype names are to be returned

          Returns:
                  DataFrame."""
                 

        getData_url = self.url + "GetData"
        raw_dataRequest = ""
        json_dataRequest = ""
        json_Response = ""
        
        if fields == None:
            fields = []
        
        try:
            req = self.post_user_request(tickers, fields, start, end, freq, kind, retName)
            datarequest = DataRequest()
            if (self.tokenResp == None):
                raise Exception("Invalid Token Value")
            elif 'Message' in self.tokenResp.keys():
                raise Exception(self.tokenResp['Message'])
            elif 'TokenValue' in self.tokenResp.keys():
               raw_dataRequest = datarequest.get_Request(req, self.dataSource, 
                                                      self.tokenResp['TokenValue'])
                #print(raw_dataRequest)
            if (raw_dataRequest != ""):
                json_dataRequest = self._json_Request(raw_dataRequest)
                #Post the requests to get response in json format
                if self._proxy and self._sslCer:
                    json_Response = requests.post(getData_url, json=json_dataRequest,
                                                  proxies=self._proxy, verify=self._sslCer).json()
                elif self._proxy:
                    json_Response = requests.post(getData_url, json=json_dataRequest,
                                                  proxies=self._proxy).json()
                elif self._sslCer:
                    json_Response = requests.post(getData_url, json=json_dataRequest,
                                                  verify=self._sslCer).json()
                else:
                    json_Response = requests.post(getData_url, json=json_dataRequest,
                                                  verify=self.certfile).json()
                #print(json_Response)
                #format the JSON response into readable table
                if 'DataResponse' in json_Response:
                    response_dataframe = self._format_Response(json_Response['DataResponse'])
                    if retName:
                        self._get_metadata(json_Response['DataResponse'])
                    return response_dataframe
                else:
                    if 'Message' in json_Response:
                        raise Exception(json_Response['Message'])
                    return None
            else:
                return None
        except json.JSONDecodeError:
            print("get_data : JSON decoder Exception Occured")
            print(traceback.sys.exc_info())
            print(traceback.print_exc(limit=5))
            return None
        except Exception:
            print("get_data : Exception Occured")
            print(traceback.sys.exc_info())
            print(traceback.print_exc(limit=5))
            return None
    
    def get_bundle_data(self, bundleRequest=None, retName=False):
        """This Function processes a multiple JSON format data requests to provide
           data response from DSWS web in the form of python Dataframe.
           Use post_user_request to form each JSON data request and append to a List
           to pass the bundleRequset.
           
            Args:
               bundleRequest: List, expects list of Datarequests 
               retName: bool, default False, to be set to True if the Instrument
                           names and Datatype names are to be returned

            Returns:
                  DataFrame."""

        getDataBundle_url = self.url + "GetDataBundle"
        raw_dataRequest = ""
        json_dataRequest = ""
        json_Response = ""
       
        if bundleRequest == None:
            bundleRequest = []
        
        try:
            datarequest = DataRequest()
            if (self.tokenResp == None):
                raise Exception("Invalid Token Value")
            elif 'Message' in self.tokenResp.keys():
                raise Exception(self.tokenResp['Message'])
            elif 'TokenValue' in self.tokenResp.keys():
                raw_dataRequest = datarequest.get_bundle_Request(bundleRequest, self.dataSource, 
                                                             self.tokenResp['TokenValue'])
            #print(raw_dataRequest)
            if (raw_dataRequest != ""):
                 json_dataRequest = self._json_Request(raw_dataRequest)
                 #Post the requests to get response in json format
                 if self._proxy and self._sslCer:
                     json_Response = requests.post(getDataBundle_url, json=json_dataRequest,
                              proxies=self._proxy, verify=self._sslCer).json()
                 elif self._proxy:
                     json_Response = requests.post(getDataBundle_url, json=json_dataRequest,
                                                  proxies=self._proxy).json()
                 elif self._sslCer:
                     json_Response = requests.post(getDataBundle_url, json=json_dataRequest,
                                                  verify=self.sslCer).json()
                 else:
                     json_Response = requests.post(getDataBundle_url, json=json_dataRequest,
                                                  verify=self.certfile).json()
                 #print(json_Response)
                 if 'DataResponses' in json_Response:
                     response_dataframe = self._format_bundle_response(json_Response)
                     if retName:
                        self._get_metadata_bundle(json_Response['DataResponses'])
                     return response_dataframe
                 else:
                    if 'Message' in json_Response:
                        raise Exception(json_Response['Message'])
                    return None
            else:
                return None
        except json.JSONDecodeError:
            print("get_bundle_data : JSON decoder Exception Occured")
            print(traceback.sys.exc_info())
            print(traceback.print_exc(limit=5))
            return None
        except Exception:
            print("get_bundle_data : Exception Occured")
            print(traceback.sys.exc_info())
            print(traceback.print_exc(limit=5))
            return None
    
#------------------------------------------------------- 
#-------------------------------------------------------             
#-------Helper Functions---------------------------------------------------
    def _get_token(self, isProxy=False):
        token_url = self.url + "GetToken"
        try:
            propties = []
            propties.append(Properties("__AppId", self.appID))
            if self.dataSource:
                propties.append(Properties("Source", self.dataSource))
            tokenReq = TokenRequest(self.username, self.password, propties)
            raw_tokenReq = tokenReq.get_TokenRequest()
            json_tokenReq = self._json_Request(raw_tokenReq)
            #Load windows certificates to a local file
            pf = platform.platform()
            if pf.upper().startswith('WINDOWS'):
                self._loadWinCerts()
            else:
                self.certfile = requests.certs.where()
            #Post the token request to get response in json format
            if self._proxy and self._sslCer:
                json_Response = requests.post(token_url, json=json_tokenReq,
                                  proxies=self._proxy, 
                                  verify=self._sslCer, timeout=10).json()
            elif self._proxy:
                json_Response = requests.post(token_url, json=json_tokenReq,
                                                  proxies=self._proxy, 
                                                  verify=False, timeout=10).json()
            elif self._sslCer:
                json_Response = requests.post(token_url, json=json_tokenReq,
                                                  verify=self._sslCer).json()
            else:
                json_Response = requests.post(token_url, json=json_tokenReq, verify=self.certfile).json()
                
            
            return json_Response
        
        except json.JSONDecodeError:
            print("_get_token : JSON decoder Exception Occured")
            print(traceback.sys.exc_info())
            print(traceback.print_exc(limit=2))
            return None
        except Exception:
            print("_get_token : Exception Occured")
            print(traceback.sys.exc_info())
            print(traceback.print_exc(limit=2))
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
        try:
            #match = re.match("^/Date[(][0-9]{13}[+][0-9]{4}[)]/", jsonDate)
            match = re.match(r"^(/Date\()(-?\d*)([+-])(..)(..)(\)/)", jsonDate)
            if match:
                #d = re.search('[0-9]{13}', jsonDate)
                d = float(match.group(2))
                ndate = datetime(1970,1,1) + timedelta(seconds=float(d)/1000)
                utcdate = pytz.UTC.fromutc(ndate).strftime('%Y-%m-%d')
                return utcdate
            else:
                raise Exception("Invalid JSON Date")
        except Exception:
            print("_get_token : Exception Occured")
            print(traceback.sys.exc_info())
            print(traceback.print_exc(limit=2))
            return None
            
            
    
    def _get_DatatypeValues(self, jsonResp):
        df = pd.DataFrame()
        multiIndex = False
        valDict = {"Instrument":[],"Datatype":[],"Value":[]}
        #print (jsonResp)
        for item in jsonResp['DataTypeValues']: 
            datatype = item['DataType']
            
            for i in item['SymbolValues']:
               instrument = i['Symbol']
               
               valDict["Datatype"].append(datatype)
               valDict["Instrument"].append(instrument)
               values = i['Value']
               valType = i['Type']
               colNames = (instrument, datatype)
               df[colNames] = None
               
               #Handling all possible types of data as per DSSymbolResponseValueType
               if valType in [7, 8, 10, 11, 12, 13, 14, 15, 16]:
                   #These value types return an array
                   #The array can be of double, int, string or Object
                   rowCount = df.shape[0]
                   valLen = len(values)
                   #If no of Values is < rowcount, append None to values
                   if rowCount > valLen:
                       for i in range(rowCount - valLen):
                            values.append(None)
                  #Check if the array of Object is JSON dates and convert
                   for x in range(0, valLen):
                       values[x] = self._get_Date(values[x]) if str(values[x]).find('/Date(') != -1 else values[x] 
                   #Check for number of values in the array. If only one value, put in valDict
                   if len(values) > 1:
                       multiIndex = True
                       df[colNames] = values
                   else:
                       multiIndex = False
                       valDict["Value"].append(values[0])   
               elif valType in [1, 2, 3, 5, 6]:
                   #These value types return single value
                   valDict["Value"].append(values)
                   multiIndex = False
               else:
                   if valType == 4:
                       #value type 4 return single JSON date value, which needs conversion
                       values = self._get_Date(values) if str(values).find('/Date(') != -1 else values
                       valDict["Value"].append(values)
                       multiIndex = False
                   elif valType == 9:
                       #value type 9 return array of JSON date values, needs conversion
                       date_array = []
                       if len(values) > 1:
                          multiIndex = True
                          for eachVal in values:
                              date_array.append(self._get_Date(eachVal)) if str(eachVal).find('/Date(') != -1 else eachVal 
                          df[colNames] = date_array
                       else:
                          multiIndex = False
                          date_array.append(self._get_Date(values[0])) if str(values[0]).find('/Date(') != -1 else values[0]
                          valDict["Value"].append(date_array[0])
                   else:
                       if valType == 0:
                           #Error Returned
                           #12/12/2019 - Error returned can be array or a single 
                           #multiIndex = False
                           valDict["Value"].append(values)
               if multiIndex:
                   df.columns = pd.MultiIndex.from_tuples(df.columns, names=['Instrument','Field'])
                   
        if not multiIndex:
            indexLen = range(len(valDict['Instrument']))
            newdf = pd.DataFrame(data=valDict,columns=["Instrument", "Datatype", "Value"],
                                 index=indexLen)
            return newdf
        return df 
            
    def _format_Response(self, response_json):
        # If dates is not available, the request is not constructed correctly
        response_json = dict(response_json)
        if 'Dates' in response_json:
            dates_converted = []
            if response_json['Dates'] != None:
                dates = response_json['Dates']
                for d in dates:
                    dates_converted.append(self._get_Date(d))
        else:
            return 'Error - please check instruments and parameters (time series or static)'
        
        # Loop through the values in the response
        dataframe = self._get_DatatypeValues(response_json)
        if (len(dates_converted) == len(dataframe.index)):
            if (len(dates_converted) > 1):
                #dataframe.insert(loc = 0, column = 'Dates', value = dates_converted)
                dataframe.index = dates_converted
                dataframe.index.name = 'Dates'
        elif (len(dates_converted) == 1):
            dataframe['Dates'] = dates_converted[0]
        
        return dataframe

    def _format_bundle_response(self,response_json):
       formattedResp = []
       for eachDataResponse in response_json['DataResponses']:
           df = self._format_Response(eachDataResponse)
           formattedResp.append(df)      
           
       return formattedResp
   
    def _loadWinCerts(self):
        import wincertstore
        cfile = wincertstore.CertFile()
        cfile.addstore('CA')
        cfile.addstore('ROOT')
        cfile.addstore('MY')
        self.certfile = cfile.name
        atexit.register(cfile.close)
        #print(self.certfile.name)
        
    def _get_metadata(self, jsonResp):
        names = {}
        if jsonResp['SymbolNames']:
            for i in jsonResp['SymbolNames']:
                names.update({i['Key']: i['Value']})
                
        if jsonResp['DataTypeNames']:
            for i in jsonResp['DataTypeNames']:
                names.update({i['Key']: i['Value']})
        
        print(names)
        
    def _get_metadata_bundle(self, jsonResp):
        for eachDataResponse in jsonResp:
            self._get_metadata(eachDataResponse)
#-------------------------------------------------------------------------------------

