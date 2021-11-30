
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
    url = "https://product.datastream.com"
    username = ""
    password = ""
    tokenResp = None
    dataSource = None
    _proxy = None
    _sslCer = None
    _timeout = 180
    appID = "PythonLib-1.1.0"
    certfile = None
   
    
#--------Constructor ---------------------------  
    def __init__(self, username, password, config=None, dataSource=None, proxy=None, sslCer= None):
        if (config):
            parser = configparser.ConfigParser()
            parser.read(config)
            self.url = self.url if parser.get('url','path').strip() == '' else parser.get('url', 'path').strip()
            self.url = self.url.lower()
            if self.url:
                if re.match("^http:", self.url):
                    self.url = self.url.replace('http:', 'https:', 1) 
            #self.url = self.url +'/DSWSClient/V1/DSService.svc/rest/'
            self._timeout = 180 if parser.get('app', 'timeout').strip() == '' else int(parser.get('app', 'timeout').strip())
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
        propList = []
        try:
            if index == -1:
                instrument = Instrument(tickers, None)
            else:
                #Get all the properties of the instrument
                instprops = []
                if tickers[index+1:].rfind(',') != -1:
                    propList = tickers[index+1:].split(',')
                    for eachProp in propList:
                        instprops.append(Properties(eachProp, True))
                else:
                    propList.append(tickers[index+1:])
                    instprops.append(Properties(tickers[index+1:], True))

                instrument = Instrument(tickers[0:index], instprops)
                        
            datypes=[]
            if 'N' in propList:
                prop = [{'Key':'ReturnName', 'Value':True}] 
                retName = True
            else:
                prop = None

            
            if len(fields) > 0:
                for eachDtype in fields:
                    datypes.append(DataType(eachDtype, prop))
            else:
                datypes.append(DataType(fields, prop))
                        
            date = Date(start, freq, end, kind)
            request = {"Instrument":instrument,"DataTypes":datypes,"Date":date}
            return request, retName
        except Exception:
            print("post_user_request : Exception Occured")
            print(traceback.sys.exc_info())
            print(traceback.print_exc(limit=5))
            return None
            
    def get_data(self, tickers, fields=None, start='', end='', freq='', kind=1):
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
            retName = False
            req, retName = self.post_user_request(tickers, fields, start, end, freq, kind, retName)
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
                json_Response = self._get_json_Response(getData_url, raw_dataRequest)
                #print(json_Response)
                #format the JSON response into readable table
                if 'DataResponse' in json_Response:
                    if retName:
                        self._get_metadata(json_Response['DataResponse'])
                    response_dataframe = self._format_Response(json_Response['DataResponse'])
                    return response_dataframe
                else:
                    if 'Message' in json_Response:
                        raise Exception(json_Response['Message'])
                    return None
            else:
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
                 json_Response = self._get_json_Response(getDataBundle_url, raw_dataRequest)
                     #print(json_Response)
                 if 'DataResponses' in json_Response:
                     if retName:
                         self._get_metadata_bundle(json_Response['DataResponses'])
                     response_dataframe = self._format_bundle_response(json_Response)
                     return response_dataframe
                 else:
                    if 'Message' in json_Response:
                        raise Exception(json_Response['Message'])
                    return None
            else:
                return None
        except Exception:
            print("get_bundle_data : Exception Occured")
            print(traceback.sys.exc_info())
            print(traceback.print_exc(limit=5))
            return None
    
#------------------------------------------------------- 
#-------------------------------------------------------             
#-------Helper Functions---------------------------------------------------
    def _get_Response(self, reqUrl, raw_request):
        try:
            #convert raw request to json format before post
            jsonRequest = self._json_Request(raw_request)
            if self._sslCer:
                http_Response = requests.post(reqUrl, json=jsonRequest, proxies=self._proxy, verify=self._sslCer, timeout= self._timeout)
            else:
                http_Response = requests.post(reqUrl, json=jsonRequest, proxies=self._proxy, verify=self.certfile, timeout= self._timeout)
            return http_Response
        except requests.exceptions.ConnectionError as conerr:
            print(conerr)
            raise
        except requests.exceptions.ConnectTimeout as conto:
            print(conto)
            raise
        except requests.exceptions.ContentDecodingError as decerr:
            print(decerr)
            raise
        except requests.exceptions.HTTPError as httperr:
            print(httperr)
            raise
        except requests.exceptions.ProxyError as prxyerr:
            print(prxyerr)
            raise
        except requests.exceptions.RequestException as reqexp:
            print(reqexp)
            raise
        except requests.exceptions.SSLError as sslerr:
            print(sslerr)
            raise
        except requests.exceptions.URLRequired as urlerr:
            print(urlerr)
            raise
        except requests.exceptions.RequestsDependencyWarning as reqwarn:
            print(reqwarn)
            raise
        except requests.exceptions.ReadTimeout as readtout:
            print(readtout)
            raise
        except requests.exceptions.InvalidHeader as inverr:
            print(inverr)
            raise
        except requests.exceptions.InvalidProxyURL as invPrxy:
            print(invPrxy)
            raise
        except requests.exceptions.InvalidURL as invurl:
            print(invurl)
            raise
        except requests.exceptions.InvalidHeader as invhdr:
            print(invhdr)
            raise
        except requests.exceptions.InvalidSchema as invschma:
            print(inschma)
            raise
        except Exception as otherexp:
            print(otherexp)
            raise

        
    def _get_json_Response(self, reqUrl, raw_request):
        try:
          httpResponse = self._get_Response(reqUrl, raw_request)
          if httpResponse:
              json_Response=dict(httpResponse.json()) if httpResponse.status_code==200 else None
              return json_Response
          else:
              return None
        except json.JSONDecodeError as jdecodeerr:
            print("_get_json_Response : JSON decoder Exception Occured: " + jdecodeerr)
            return None
        except Exception as exp:
            print("_get_json_Response : Exception Occured: ")
            print(exp)
            return None
    
    def _get_token(self, isProxy=False):
        token_url = self.url + "GetToken"
        try:
            propties = []
            propties.append(Properties("__AppId", self.appID))
            if self.dataSource:
                propties.append(Properties("Source", self.dataSource))
            tokenReq = TokenRequest(self.username, self.password, propties)
            raw_tokenReq = tokenReq.get_TokenRequest()
            
            #Load windows certificates to a local file
            pf = platform.platform()
            if pf.upper().startswith('WINDOWS'):
                self._loadWinCerts()
            else:
                self.certfile = requests.certs.where()

            #Post the token request to get response in json format
            json_Response = self._get_json_Response(token_url, raw_tokenReq)
            return json_Response
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
        valDict = {"Instrument":[],"Datatype":[],"Value":[],"Currency":[]}
        #print (jsonResp)
        for item in jsonResp['DataTypeValues']: 
            datatype = item['DataType']
            
            for i in item['SymbolValues']:
               instrument = i['Symbol']
               currency = None
               if 'Currency' in i:
                   currency = i['Currency'] if i['Currency'] else 'NA'

               valDict["Datatype"].append(datatype)
               valDict["Instrument"].append(instrument)
               if currency:
                   valDict['Currency'].append(currency)
                   colNames = (instrument, datatype, currency)
               else:
                   colNames = (instrument, datatype)
               values = i['Value']
               valType = i['Type']
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
                           df[colNames] = values
               if multiIndex:
                   if currency:
                        df.columns = pd.MultiIndex.from_tuples(df.columns, names=['Instrument','Field','Currency'])
                   else:
                        df.columns = pd.MultiIndex.from_tuples(df.columns, names=['Instrument','Field'])
                   
        if not multiIndex:
            indexLen = range(len(valDict['Instrument']))
            if valDict['Currency']:
                newdf = pd.DataFrame(data=valDict,columns=["Instrument", "Datatype", "Value", "Currency"],
                                 index=indexLen)
            else:
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

