# PyDSWS
Python wrapper for the Datastream Web Services API (DSWS)

Connect to the Thomson Reuters Datastream database via Datastream Web Services. 
You need to have a Datastream subscription and a username/password to use this package.
Please note that this is an official package and it is still under development. 
For support on this package, please contact Thomson Reuters team.
The package has basic functionality and most of the error handling still needs to be done.

Requirements:
----------------------------------------------------------------------------------
Install Python 3 on your machine
Packages to be installed:
--------------------------------
pandas
requests
datetime
pytz


Package Installation:
----------------------------------------------------------------------------------
pip install PyDSWS_Wrapper
----------------------------------------------------------------------------------

### Usage
----------------------------------------------------------------------------------
1) import the 'PyDSWS_Wrapper' package
2) authenticate with your username and password
----------------------------------------------------------------------------------
3) Using get_data
----------------------------------------------------------------------------------
import PyDSWS_Wrapper as pw

ds = pw.DataStream(username='XXXXXXX', password='XXXXXXX')
df = ds.get_data(tickers='VOD', fields=['P'], start ='2017-01-01', end = '-5D')
print(df)

For static data:
----------------------------------------------------------------------------------
df = ds.get_data(tickers='VOD', fields=['VO','P'], start='2017-01-01', kind = 0)

Output:
 Instrument Datatype     Value       Dates
0        VOD       VO  36773.80  2017-01-01
1        VOD        P    199.85  2017-01-01

For time series:
----------------------------------------------------------------------------------
df = ds.get_data(tickers='VOD', fields=['P','MV','VO'], start='-10D', end='-0D', freq='D')

Output:
Instrument     VOD
Field            P        MV       VO
Date
2017-11-21  229.75  61283.06  55100.4
2017-11-22  228.75  61016.34  79602.5
2017-11-23  225.40  60122.75  35724.1
2017-11-24  225.50  60149.44  42918.0
2017-11-27  224.60  59909.38  50355.3
2017-11-28  226.45  60402.83  49027.0
2017-11-29  225.25  60082.74  61618.1
2017-11-30  224.30  59829.99  95423.4
2017-12-01  224.00  59749.96  54855.4

----------------------------------------------------------------------------------
4) Using get_bundle_data
----------------------------------------------------------------------------------
ds = DataStream("xxxxxxx", "xxxxxxxxx")
reqs =[]
reqs.append(ds.post_user_request(tickers='VOD',fields=['VO','P'],start='2017-01-01', kind = 0))#ststic data
reqs.append(ds.post_user_request(tickers='U:BAC', fields=['P'], start='1975-01-01', end='0D', freq = "Y"))#Timeseries data
df = ds.get_bundle_data(bundleRequest=reqs)
print(df)

Instrument Datatype     Value       Dates
0        VOD       VO  36773.80  2017-01-01
1        VOD        P    199.85  2017-01-01, 
Instrument       Dates    U:BAC
Field                         P
0           1975-01-01   0.9375
1           1976-01-01   1.2188
2           1977-01-01   1.5313
3           1978-01-01   1.4219
.....

----------------------------------------------------------------------------------
5) Retrieving data for a List
----------------------------------------------------------------------------------
import PyDSWS_Wrapper as pw
dst = pw pw.DataStream(username="xxxxx", password="xxxxx")

df = ds.get_data(tickers="LS&PCOMP|L",fields =["NAME"])
print(df)

Note that we should specify |L in tickers, for List.

Output:
    Instrument Datatype                    Value       Dates
0        891399     NAME               AMAZON.COM  2019-01-21
1        916328     NAME      ABBOTT LABORATORIES  2019-01-21
2        545101     NAME                      AES  2019-01-21
3        777953     NAME                  ABIOMED  2019-01-21
......

----------------------------------------------------------------------------------
6) Retrieving data for Expressions
----------------------------------------------------------------------------------
import PyDSWS_Wrapper as pw
dst = pw pw.DataStream(username="xxxxx", password="xxxxx")

df = ds.get_data(tickers='PCH#(VOD(P),3M)|E', start="20181101",end="-1M", freq="M")
print(df)

Note that we should specify |E in tickers, for Expressions.

Output:
Instrument       Dates PCH#(VOD(P), 3M)
Field                                  
0           2018-11-01           -17.82
1           2018-12-01             0.91

Using Symbol substitution:
-------------------------------------------
df =ds.get_data(tickers='VOD, U:JPM',fields=['PCH#(X(P),-3M)'], freq="M")

Instrument       Dates            VOD          U:JPM
Field                  PCH#(X(P),-3M) PCH#(X(P),-3M)
0           2018-02-07          -3.07        14.2987
1           2018-03-07         -10.25         9.6635
2           2018-04-07         -13.85         0.6923
3           2018-05-07           0.24        -3.1009
4           2018-06-07          -8.30        -3.4254
5           2018-07-07          -6.37        -4.6109
......

----------------------------------------------------------------------------------
7) Retrieving data for NDOR
----------------------------------------------------------------------------------
df = ds.get_data(tickers='USGDP…D',fields=['DS.NDOR1'])

Output:
Instrument              Datatype       Value
0  USGDP...D         DS.NDOR1_DATE  2019-02-11
1  USGDP...D  DS.NDOR1_DATE_LATEST  2019-02-19
2  USGDP...D     DS.NDOR1_TIME_GMT          NA
3  USGDP...D    DS.NDOR1_DATE_FLAG   Estimated
4  USGDP...D   DS.NDOR1_REF_PERIOD  2018-11-15
5  USGDP...D         DS.NDOR1_TYPE    NewValue

----------------------------------------------------------------------------------
8) Retrievung data for Point In Time
----------------------------------------------------------------------------------
df = ds.get_data(tickers='CNCONPRCF(DREL1)', fields=['(X)'], start='-2Y', end='0D', freq='M')

Output:
Instrument       Dates CNCONPRCF(DREL1)
Field                               (X)
0           2017-02-15       2017-03-24
1           2017-03-15       2017-04-21
2           2017-04-15       2017-05-19
3           2017-05-15       2017-06-23
4           2017-06-15       2017-07-21
5           2017-07-15       2017-08-18

----------------------------------------------------------------------------------
9) Usage Stats
----------------------------------------------------------------------------------
df = ds.get_data(tickers='STATS', fields=['DS.USERSTATS'], kind=0)

Output:
Instrument    Datatype       Value       Dates
0      STATS        User     ZDSM042  2019-02-08
1      STATS        Hits         147  2019-02-08
2      STATS    Requests         113  2019-02-08
3      STATS   Datatypes         660  2019-02-08
4      STATS  Datapoints       23213  2019-02-08
5      STATS  Start Date  2019-02-01  2019-02-08
6      STATS    End Date  2019-02-28  2019-02-08
