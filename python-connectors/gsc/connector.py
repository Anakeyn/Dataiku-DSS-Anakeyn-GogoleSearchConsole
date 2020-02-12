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
            #beware !! dates are in string in the forms
            self.from_date = datetime.strptime( self.config.get("from_date")[:10], '%Y-%m-%d')
            self.to_date = datetime.strptime( self.config.get("to_date")[:10], '%Y-%m-%d')

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
        #Get Data  Pages/Queries/positions from Google Search Console
        ###############################################################################

        
        dfGSC = pd.DataFrame()  #global dataframe
        #convert dates in strings
        myStrStartDate = self.from_date.strftime('%Y-%m-%d') 
        myStrEndDate = self.to_date.strftime('%Y-%m-%d') 

        
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
