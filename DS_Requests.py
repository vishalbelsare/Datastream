# -*- coding: utf-8 -*-
"""
Created on Sat Dec 29 00:55:39 2018

@author: Vidya Dinesh
"""
#--------------------------------------------------------------------------------
class IProperties:
    """Properties of Instruments"""
    Key = ""
    Value = True
    
    def __init__(self, key, value):
        self.Key = key
        self.Value = value
        
#--------------------------------------------------------------------------------      
class DataType:
    """Class used to store Datatype""" 
    datatype = ""
    
    def __init__(self, value):
       self.datatype = value

#--------------------------------------------------------------------------------      
class Date:
    """Date parameters of a Data Request"""
    Start = ""
    End = ""
    Frequency = ""
    Kind = 0
    
    def __init__(self, startDate = "", freq = "D", endDate = "", kind = 0):
       self.Start = startDate
       self.End = endDate
       self.Frequency = freq
       self.Kind = kind

#--------------------------------------------------------------------------------                  
class Instrument(IProperties):
    """Instrument and its Properties"""
    instrument = ""
    properties = [IProperties]
    
    def __init__(self, inst, props):
        self.instrument = inst
        self.properties = props
    
#--------------------------------------------------------------------------------
class Properties:
    """Properties of Data Request"""
    """Captures the data source given in the Request,it
    can be "PROD"/ "STAGING" / "QA". If not specified, 
    a Default source is taken"""
    
    Key = "Source"
    Value = ""
    
    def __init__(self, value):
        self.Value = value

#--------------------------------------------------------------------------------
#--------------------------------------------------------------------------------
#--------------------------------------------------------------------------------
"""Classes that help to form the Request in RAW JSON format"""
class TokenRequest(Properties):
    password = ""
    username = ""
    
    def __init__(self, uname, pword, source = None):
        self.username = uname
        self.password = pword
        self.Key = "Source"
        self.Value = source
        
    def get_TokenRequest(self):
        tokenReq = {"Password":self.password,"Properties":[],"UserName":self.username}
        if self.Value == None or self.Value == "":
            tokenReq["Properties"] = None
        else:
            tokenReq["Properties"].append({"Key":self.Key,"Value":self.Value})
        
        return tokenReq
#--------------------------------------------------------------------------------
class DataRequest:
     
    hints = {"E":"IsExpression", "L":"IsList"}
    singleReq = dict
    multipleReqs = dict
    
    def __init__(self):
        self.singleReq = {"DataRequest":{},"Properties":None,"TokenValue":""}
        self.multipleReqs = {"DataRequests":[],"Properties":None,"TokenValue":""}
    
    def get_bundle_Request(self, reqs, source=None, token=""):
        self.multipleReqs["DataRequests"] = []
        for eachReq in reqs:
            dataReq = {"DataTypes":[],"Instrument":{}, "Date":{}, "Tag":None}
            dataReq["DataTypes"] = self._set_Datatypes(eachReq["DataTypes"])
            dataReq["Date"] = self._set_Date(eachReq["Date"])
            dataReq["Instrument"] = self._set_Instrument(eachReq["Instrument"])
            self.multipleReqs["DataRequests"].append(dataReq)
            
        self.multipleReqs["Properties"] = {"Key":"Source","Value":source}
        self.multipleReqs["TokenValue"] = token
        return self.multipleReqs
        
        
    def get_Request(self, req, source=None, token=""):
        dataReq = {"DataTypes":[],"Instrument":{}, "Date":{}, "Tag":None}
        dataReq["DataTypes"] = self._set_Datatypes(req["DataTypes"])
        dataReq["Date"] = self._set_Date(req["Date"])
        dataReq["Instrument"] = self._set_Instrument(req["Instrument"])
        self.singleReq["DataRequest"] = dataReq
        
        self.singleReq["Properties"] = {"Key":"Source","Value":source}
        self.singleReq["TokenValue"] = token
        return self.singleReq
    
#--------------------HELPER FUNCTIONS--------------------------------------      
    def _set_Datatypes(self, dtypes=None):
        """List the Datatypes"""
        datatypes = []
        for eachDtype in dtypes:
            if eachDtype.datatype == None:
                continue
            else:
                datatypes.append({"Properties":None, "Value":eachDtype.datatype})
        return datatypes
            
        
    def _set_Instrument(self, inst):
        propties=[]
        if inst.properties == None:
            return {"Properties":None,"Value":inst.instrument}
        else:
            for eachPrpty in inst.properties:
                propties.append({"Key":DataRequest.hints[eachPrpty.Key],"Value":True})
            return {"Properties":propties,"Value":inst.instrument}
            
    
    def _set_Date(self, dt):
        return {"End":dt.End,"Frequency":dt.Frequency,"Kind":dt.Kind,"Start":dt.Start}
 #--------------------------------------------------------------------------        
    
   

            
    
##Datatypes
#dat =[]
#dat.append(DataType("PH"))
#dat.append(DataType("PL"))
#dat.append(DataType("P"))
##Instrument
#Props = [IProperties("E", True)]
#ins = Instrument("VOD", Props)
#ins2 = Instrument("U:F", Props)
##Date
#dt = Date(startDate = "20180101",freq= "M",kind = 1)
#
#dr = DataRequest()
#req1 = {"DataTypes":dat,"Instrument":ins,"Date":dt}
#req2 = {"DataTypes":dat,"Instrument":ins2,"Date":dt}
#datareq = dr.get_Request(req=req1, source='PROD',token='token')
#print(datareq)




    
    


    
    
        
        
    

