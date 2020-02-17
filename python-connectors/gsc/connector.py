#for Dataiku
from dataiku.connector import Connector
#for Dataframes
import pandas as pd  
#Datetime tools
from datetime import date
from datetime import datetime
from datetime import timedelta
import os  #to read files
import json #to manipulate json 
#for Google Search Console Reporting API V3 with service account
from apiclient.discovery import build  #from google-api-python-client
from oauth2client.service_account import ServiceAccountCredentials  #to use a Google Service 

#my Connector Class
class MyConnector(Connector):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user in the settings tab of the
        dataset are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class

        
        #perform some more initialization
        SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
        #DISCOVERY_URI = ('https://www.googleapis.com/discovery/v1/apis/customsearch/v1/rest') #not used 


        #Plugin parameters (declare in plugin.json )
        self.credentials = self.plugin_config.get("credentials") 
        self.webSite = self.plugin_config.get("webSite")
        #Component parameters (deckare in connector.json)
        
        self.period = self.config.get("period")
        print(self.period)
        if (self.period=="1day") :
            self.from_date = date.today() - timedelta(days=2)
            self.to_date = date.today() - timedelta(days=1)
        if (self.period=="7days") :
            self.from_date = date.today() - timedelta(days=8)
            self.to_date = date.today() - timedelta(days=1)
        if (self.period=="28days") :
            self.from_date = date.today() - timedelta(days=29)
            self.to_date = date.today() - timedelta(days=1)          
        if (self.period=="3months") :
            self.from_date = date.today() - timedelta(days=30*3)
            self.to_date = date.today() - timedelta(days=1) 
        if (self.period=="6months") :
            self.from_date = date.today() - timedelta(days=30*6)
            self.to_date = date.today() - timedelta(days=1)            
        if (self.period=="12months") :
            self.from_date = date.today() - timedelta(days=30*12)
            self.to_date = date.today() - timedelta(days=1) 
        if (self.period=="16months") :
            self.from_date = date.today() - timedelta(days=30*16)
            self.to_date = date.today() - timedelta(days=1)                         
        if (self.period=="Personalized") :
            #beware !! dates are in string in the forms, and there is a shift error of one minus day
            self.from_date = datetime.strptime( self.config.get("from_date")[:10], '%Y-%m-%d')
            #avoid shift error
            self.from_date = self.from_date + timedelta(days=1)             
       
            self.to_date = datetime.strptime( self.config.get("to_date")[:10], '%Y-%m-%d')
            #avoid shift error
            self.to_date = self.to_date + timedelta(days=1) 
        
        if self.to_date < self.from_date:
            raise ValueError("The end date occurs before the start date")  
        
        #get JSON Service account credentials from file or from text 
        file = self.credentials.splitlines()[0]
        if os.path.isfile(file):
            try:
                with open(file, 'r') as f:
                    self.credentials  = json.load(f)
                    f.close()
            except Exception as e:
                raise ValueError("Unable to read the JSON Service Account from file '%s'.\n%s" % (file, e))
        else:
            try:
                self.credentials  = json.loads(self.credentials)
            except Exception as e:
                raise Exception("Unable to read the JSON Service Account.\n%s" % e)
        
        #get credentials from service account
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(self.credentials, SCOPES)

        #open a Google Search console service (previously called Google Webmasters tools)
        self.webmasters_service = build('webmasters', 'v3', credentials=credentials)

        
        

    def get_read_schema(self):
        """
        Returns the schema that this connector generates when returning rows.

        The returned schema may be None if the schema is not known in advance.
        In that case, the dataset schema will be infered from the first rows.

        If you do provide a schema here, all columns defined in the schema
        will always be present in the output (with None value),
        even if you don't provide a value in generate_rows

        The schema must be a dict, with a single key: "columns", containing an array of
        {'name':name, 'type' : type}.

        Example:
            return {"columns" : [ {"name": "col1", "type" : "string"}, {"name" :"col2", "type" : "float"}]}

        Supported types are: string, int, bigint, float, double, date, boolean
        """

        # In this example, we don't specify a schema here, so DSS will infer the schema
        # from the columns actually returned by the generate_rows method
        return None
    

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """
        
    
        print ("Google Search Console plugin - Start generating rows")
        print ("Google Search Console plugin - records_limits=%i" % records_limit)


        ###############################################################################
        #Get Data  Pages/Queries/positions/Clicks from Google Search Console
        ###############################################################################

        dfAllTraffic = pd.DataFrame()  #global dataframe for all traffic calculation
        dfGSC = pd.DataFrame()  #global dataframe for clicks
        #convert dates in strings
        myStrStartDate = self.from_date.strftime('%Y-%m-%d') 
        myStrEndDate = self.to_date.strftime('%Y-%m-%d') 

         
        ####### Get Global Traffic ##############

        
        maxStartRow = 1000000000 #to avoid infinite loop
        myStartRow = 0
        
  
        
        while ( myStartRow < maxStartRow):
            
            df = pd.DataFrame() #dataframe for this loop
            
            
            mySiteUrl = self.plugin_config.get("webSite")
            myRequest = {
                'startDate': myStrStartDate,    #older date
                'endDate': myStrEndDate,      #most recent date
                'dimensions': ["date", "country", "device"],
                'searchType': 'web',         #for the moment only Web 
                'rowLimit': 25000,         #max 25000 for one Request 
                "aggregationType": "byPage",
                'startRow' :  myStartRow                #  for multiple resquests 'startRow':
                }

            response =  self.webmasters_service.searchanalytics().query(siteUrl=mySiteUrl, body=myRequest).execute()


            
            #set response (dict) in DataFrame for treatments purpose.
            df = pd.DataFrame.from_dict(response['rows'], orient='columns')

            if ( myStartRow == 0) :
                dfAllTraffic = df  #save the first loop df in global df
            else :
                dfAllTraffic = pd.concat([dfAllTraffic, df], ignore_index=True) #concat  this loop df  with  global df

            if (df.shape[0]==25000) :
                myStartRow += 25000  #continue
            else :
                 myStartRow = maxStartRow+1  #stop
        
        #split keys in date country device
        dfAllTraffic[["date", "country", "device"]] = pd.DataFrame(dfAllTraffic["keys"].values.tolist())
        dfAllTraffic =  dfAllTraffic.drop(columns=['keys'])  #remove Keys (not used)
        
        myTotalClicks = dfAllTraffic['clicks'].sum()
        myTotalImpressions = dfAllTraffic['impressions'].sum()
        
        
        
         ####### Get Pages/Queries/positions/Clicks ##############
        
        maxStartRow = 1000000000 #to avoid infinite loop
        myStartRow = 0
        
  
        
        while ( myStartRow < maxStartRow):
            
            df = pd.DataFrame() #dataframe for this loop
            
            
            mySiteUrl = self.plugin_config.get("webSite")
            myRequest = {
                'startDate': myStrStartDate,    #older date
                'endDate': myStrEndDate,      #most recent date
                'dimensions':  ["date", "query","page","country","device"],      #all available dimensions ?
                'searchType': 'web',         #for the moment only Web 
                'rowLimit': 25000,         #max 25000 for one Request 
                'startRow' :  myStartRow                #  for multiple resquests 'startRow':
                }

            response =  self.webmasters_service.searchanalytics().query(siteUrl=mySiteUrl, body=myRequest).execute()


            
            #set response (dict) in DataFrame for treatments purpose.
            df = pd.DataFrame.from_dict(response['rows'], orient='columns')

            if ( myStartRow == 0) :
                dfGSC = df  #save the first loop df in global df
            else :
                dfGSC = pd.concat([dfGSC, df], ignore_index=True) #concat  this loop df  with  global df

            if (df.shape[0]==25000) :
                myStartRow += 25000  #continue
            else :
                 myStartRow = maxStartRow+1  #stop
        
        #split keys in date query page country device
        dfGSC[["date", "query", "page", "country", "device"]] = pd.DataFrame(dfGSC["keys"].values.tolist())
        dfGSC =  dfGSC.drop(columns=['keys'])  #remove Keys (not used)
        
        
        mySampleClicks = dfGSC['clicks'].sum()
        mySampleImpressions = dfGSC['impressions'].sum()
                
        
        #Recalculate new clicks and Impressions
        #recalculate All Clicks according to clicks volume ratio (we privilegiate clicks accuracy)
        dfGSC['allClicks'] = dfGSC.apply(lambda x : round((x['clicks']*myTotalClicks)/mySampleClicks, 0),axis=1)
        #Recalculate news All Impressions according to clicks volume ratio
        dfGSC['allImpressions'] = dfGSC.apply(lambda x : round((x['impressions']*myTotalClicks)/mySampleClicks, 0),axis=1) 
        #Reclaculate news All ctr according to new All impressions and Clicks
        dfGSC['allCTR'] = dfGSC.apply(lambda x : x['allClicks']/x['allImpressions'],axis=1) 
         
        #remove bad dates 
        #Change string date in datetime
        dfGSC['date'] = dfGSC.apply(lambda x : datetime.strptime( x['date'][:10], '%Y-%m-%d'),axis=1) 
        mask = (dfGSC['date'] >= self.from_date) & (dfGSC['date'] <= self.to_date)
        dfGSC = dfGSC.loc[mask]    
        dfGSC.reset_index(inplace=True, drop=True)  #reset index
        
        #remove old clicks, ctr and impression columns
        dfGSC =  dfGSC.drop(columns=['clicks', 'ctr', 'impressions'])
        #rename All impressions, ctr and clicks in old names
        dfGSC.rename(columns={'allImpressions':'impressions', 'allClicks':'clicks', 'allCTR':'ctr'}, inplace=True)
        
        #reorganise in orignal order clicks, ctr, impressions, positions, date, query, page, country, device
        dfGSC = dfGSC[["clicks", "ctr", "impressions", "position", "date", "query", "page", "country", "device"]]
        #send rows got in dataframe transformed in dict 
        for row in dfGSC.to_dict(orient='records'):
            yield row  #Each yield in the generator becomes a row in the dataset.



    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):
        """
        Returns a write object to write in the dataset (or in a partition)

        The dataset_schema given here will match the the rows passed in to the writer.

        Note: the writer is responsible for clearing the partition, if relevant
        """
        raise Exception("Unimplemented")


    def get_partitioning(self):
        """
        Return the partitioning schema that the connector defines.
        """
        raise Exception("Unimplemented")

    def get_records_count(self, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the field "canCountRecords" is set to
        true in the connector.json
        """
    
        raise Exception("unimplemented")
