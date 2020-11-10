#these are the modules we use to access the API
import json
import urllib.request

#these are the modules we use to manipulate the data/data handling/data validation processes  
import pandas as pd
import math

#these are the packages we use to scrape the filing documents
from bs4 import BeautifulSoup
import requests
#this, in particular, is a package we use to speed up the slow process of sending lots of requests at once (for scalability)
import concurrent.futures

#it may be undesirable to have the API key written directly in the script so we keep it hidden in a config file
import os
from config import MY_API_KEY

#design an 'Extractatron' object in which we will do the majority of our coding
class Extractatron() :
    
    API_KEY = MY_API_KEY
    API_URL = f"https://api.sec-api.io?token={API_KEY}"
    query_string = ""
    r_obj = None
    matching_filings = 0 
    active_data = None
    initiated = False
    df = None

    def __init__(self, API_columns, filing_columns) :
        '''Given two distinct lists of data points (in a specific format), we create an object designed to
        help us extract. 

        Example:
        1.) Initiate the object with the desired column inputs.
        2.) run .generate_query()
        3.) run .mass_extract_from_API()
        4.) set indexed_soups = .request_filing_data()
        5.) run .parse_filing_documents(indexed_soups, restrictions)
        For these methods in particular, look at each of their individual documentation.
        
        PARAMETERS:
        API_columns : what data do we want to get directly from the API? ie. CIK, companyName, etc.
        filing_columns : what data do we want to get from the filing documents? ie. Industry Group, City'''
        
        self.API_columns = API_columns
        self.filing_columns = filing_columns
        self.columns = API_columns + filing_columns
        self.df = pd.DataFrame(columns = self.columns + ['Required Data Point?'])

    def generate_query(self, cik = None, ticker = None, companyName = None, formType = None, filedAt = None) :
        '''This method generates the desired query string, storing it internally within the attribute .query_string
        
        PARAMETERS
        cik: 118421 (ex.)
        ticker:
        companyName: name of company
        formType: D, 10-Q (ex.)
        filedAt: {2020-01-01 TO 2020-01-15}

        This query string is written in Lucene syntax. 
        A tutorial can be found: http://www.lucenetutorial.com/lucene-query-syntax.html'''
        query_parameters = []
        if cik is not None :
            query_parameters.append(f"cik:{str(cik)}")
        if ticker is not None :
            query_parameters.append(f"ticker:{ticker}")
        if companyName is not None :
            query_parameters.append(f"companyName:{companyName}")
        if formType is not None :
            query_parameters.append(f"formType:{formType}")
        if filedAt is not None :
            query_parameters.append(f"filedAt:{filedAt}")
        
        query_string = " AND ".join(query_parameters)
        self.query_string = query_string

    def generate_payload(self, start=0, length=200) :
        '''This method returns the 'payload' information we plug into the API. 
        
        PARAMETERS
        start: of the N matching filings, where do we want to start? ie. if start = 10, query will ignore first 9 entries.
        length: how many entries in total do we wish to retrieve? Note: max 200.

        Further documentation on specifics is available at https://sec-api.io/docs'''

        #firstly check that the parameters are valid inputs
        if isinstance(length, int) and isinstance(start, int) :
            pass
        else :
            print("This function takes only integer arguments.")
            return(None)
        
        payload = {
            "query": { "query_string": { "query" : self.query_string } },
            "from": str(start),
            "size": str(start+length),
            "sort": [{ "filedAt": { "order": "desc" } }]
        }
        return(payload)

    def instantiate_extraction(self) :
        '''This method initiates the urllib.request.Request object. This method is in-built into other methods.'''
        self.r_obj = urllib.request.Request(self.API_URL)
        self.r_obj.add_header('Content-Type', 'application/json; charset=utf-8')

    def individual_extraction_from_API(self, start=0) :
        '''This method generates a batch of (200 rows max) data for the saved query string (run .generate_query()).
        This data is stored in the attribute .active_data, and we switch on .initiated = True.
        This method is unlikely to be called directly, as it is implicitly called within .mass_extraction_from_API().
        
        PARAMETERS
        start: of the N matching filings, where do we want to start? ie. if start = 10, query will ignore first 9 entries.

        Further documentation on specifics is available at https://sec-api.io/docs'''

        jsondata = json.dumps(self.generate_payload(start, 200))
        jsondataasbytes = jsondata.encode('utf-8')
        
        #check if instante_extraction method has been called, and if not, call it
        if self.r_obj is None :
            self.instantiate_extraction()
        else :
            pass
            
        self.r_obj.add_header('Content-Length', len(jsondataasbytes))
        response = urllib.request.urlopen(self.r_obj, jsondataasbytes)
        res_body = response.read()
        data = json.loads(res_body.decode("utf-8"))
        
        #store this data in the attribute 'active_data'
        self.active_data = data
        
        #let the system know that we have begun extraction
        self.initiated = True

    def mass_extract_from_API(self) :
        '''This method performs a mass extraction throughout the matching filings (wrt the previously generated query string).
        It subsequently extracts the data pieces given at the __init__ stage in the API_columns list parameter.
        
        PARAMETERS
        No parameters. However, requires prior use of .generate_query() method.'''
        
        #consider adding an actual check that the query string is non-empty (ie we have ran .generate_query())
        
        #check if we have taken the first batch of data, and if not, then initiate
        if self.initiated == True :
            pass
        else :
            self.individual_extraction_from_API()
        
        #if we are looking for a specific formType then we need to double check the form is correct
        #this is because the query recognises '10-D' as satisfying 'D'
        if "formType" in self.query_string :
            #this is potentially cumbersome
            formType = self.query_string.split("formType:")[1].split(" ")[0]
        else :
            formType = None

        N=0
        while len(self.active_data['filings']) > 0 :
            for Dictionary in self.active_data['filings'] :
                #filter for formType cos the query search is insufficient
                if formType is not None and formType != Dictionary['formType'] :
                    continue
                else :
                    pass
                
                new_data = {}
                #what data do we want? ideally we'd like to loop through self.API_columns
                #however, this would require a full classification of all possibilities
                if 'CIK' in self.API_columns :
                    new_data['CIK'] = Dictionary['cik']
                #get companyName
                if 'Company Name' in self.API_columns :
                    new_data['Company Name'] = Dictionary['companyName']
                #get link to filing details
                if 'Filing Details URL' in self.API_columns :
                    new_data['Filing Details URL'] = Dictionary['linkToFilingDetails']

                #add this row of data to the df
                self.df = self.df.append(new_data, ignore_index = True)

            #now move onto the next batch
            
            N+=1
            self.individual_extraction_from_API(N*200)
        print(f"In total we had an output of: {N}")

    def request_filing_data(self) :
        '''This method returns an dictionary of 'indexed_soups'. Using multithreading techniques,
        we request the html_content of each of the filing document URLs, convert into a BeatifulSoup object,
        and store it in an indexed dictionary to ensure 'order' is not confused in the threading process.

        PARAMETERS
        No parameters. However, this method should be called only after .mass_extraction_from_API(),
        or after .individual_extraction_from_API().'''

        #collect a list of urls, noting the ordering is the same as the df indexing
        url_list = self.df['Filing Details URL'].tolist()
        
        indexed_soups = {}
        #here we plan to use multithreading and so first we define a function for an individual thread
        def individual_function(url, indexed_soups = indexed_soups) :
            index = url_list.index(url)
            html_content = requests.get(url).text
            indexed_soups[index] = html_content

        if __name__ == "__main__" :
            #here we perform this function concurrently along our list of urls
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor :
                executor.map(individual_function, url_list)
        #note that the way we constructed the individual_function, data has now been added to indexed_soups

        #then we use BeautifulSoup to help us parse the html
        #(this step could potentially be sped up with multi processing tools, but for now it is fine)
        indexed_soups = {index : BeautifulSoup(html_content, "lxml") for index, html_content in indexed_soups.items()}

        #note: this may need to be reordered slightly as a process if the data gets so vast that it is necessary to gather it in chunks
        #for now, though, with quantities of data like this, we're fine as is
        return(indexed_soups)

    def parse_filing_documents(self, indexed_soups, restrictions) :
        '''This method takes an 'indexed_soup' and, with a set of restrictions, collects the appropriate data
        from the filing document HTML content. The restrictions parameter is designed to help us progress with future projects.
        The data in particular we look for was decided at initiation (filing_columns) and is stored in the .df attribute.
        
        PARAMETERS
        indexed_soups: this will be the object returned by .request_filing_data()
        restrictions: a list object of extra criteria for data we wish to consider
        
        Further documentation of BeautifulSoup can be found at https://www.crummy.com/software/BeautifulSoup/bs4/doc/'''


        for index, soup in indexed_soups.items() :
            
            #note: the following is designed so that it can easily be reused/added to. here we might seek to have a list of restrictions
            #and a list of filing_columns, the former filtering out unwanted data and the latter retrieving the data in question
            #there are various ways to make this more reusable/adaptable to other tasks but that is likely outside the scope of this task
            
            #firstly let's make cuts based on our restrictions parameters
            if 'year_of_incorp=2020' in restrictions :
                year_of_incorp = soup.find("table", attrs = {"summary" : "Year of Incorporation/Organization"}).find_all("span", attrs = {"class" : "FormData"})[-1].text

                if year_of_incorp == '2020' :
                    pass
                else :
                    continue

            if 'investment_group=pooled_investment_fund, no other' in restrictions :
                industry_type_info = soup.find("table", attrs= {"summary" : "Industry Group, Banking & Financial Services"}).find_all("span", attrs = {"class" : "FormData"})
            
                if len(industry_type_info) >= 2 :
                    pass
                else :
                    continue

                text = [x.find_parent('tr').find('td', attrs={'class' : 'FormText'}).text for x in industry_type_info][:2]                
                
                if text[0] == 'Pooled Investment Fund' and text[1] != 'Other Investment Fund' :
                    pass

                else :
                    continue
            

            #now we look for the data we have requested in the filing_columns parameters
            new_data = {}
            #fill the required data into the new_data dictionary
            if 'Entity Type' in self.filing_columns :
                entity_type = soup.find("table", attrs = {"summary" : "Issuer Identity Information"}).find("table", attrs = {"summary" : "Table with Multiple boxes"}).find("span", attrs = {"class" : "FormData"}).find_parent("tr").find_all("td")[-1].text
                new_data['Entity Type'] = entity_type
            
            if 'Pooled Investment Fund Type' in self.filing_columns :
                industry_type_info = soup.find("table", attrs= {"summary" : "Industry Group, Banking & Financial Services"}).find_all("span", attrs = {"class" : "FormData"})
                text = [x.find_parent('tr').find('td', attrs={'class' : 'FormText'}).text for x in industry_type_info][:2]
                new_data['Pooled Investment Fund Type'] = text[1]

            if 'Principal Place of Business (City)' in self.filing_columns :
                city = soup.find("table", attrs = {"summary" : "Principal Place of Business and Contact Information"}).find_all("tr")[-1].find("td").text
                new_data['Principal Place of Business (City)'] = city

            #note: in the longterm it would be better to loop through filing_columns, restrictions and figure out the desired action
            #however, this requires a lot of consideration for all different possibilities
            
            #here we note that we can do a quick validation of 'matching' between direct API and the filing documents themselves
            #I have simply added an alert that the validation failed and the name of the company, for now. 
            #An important extension would be 'what might we want to do in the case that this validation fails?
            if 'CIK' in self.API_columns :
                cik_for_validation = soup.find("table", attrs = {"summary": "Issuer Identity Information"}).find_all("tr")[2].find("a").text
                if int(cik_for_validation) != int(self.df.loc[index]['CIK']) :
                    print("validation failed: " + self.df.loc[index]['Company Name'])

            #fill the dataframe
            for column in self.filing_columns :
                self.df[column][index] = new_data[column]    
            self.df['Required Data Point?'][index] = 'yes'
        
        #shave off unnecessary data points
        self.df = self.df[self.df['Required Data Point?'] == 'yes']

        #drop the now unnecessary column of 'Required Data Point'
        self.df.drop(columns = 'Required Data Point?', inplace=True)

        #reset the indices
        self.df.reset_index(inplace=True)
        self.df.drop(columns = 'index', inplace=True)

#instantiate our object
Extractosaur = Extractatron(API_columns = ['CIK', 'Company Name', 'Filing Details URL'], filing_columns = ['Entity Type', 'Principal Place of Business (City)', 'Pooled Investment Fund Type'])

#we choose our desired query parameters
formType = "D"
filedAt = "{2020-06-01 TO 2020-06-30}"

#run the script
Extractosaur.generate_query(formType=formType, filedAt=filedAt)
Extractosaur.mass_extract_from_API()
indexed_soups = Extractosaur.request_filing_data()
Extractosaur.parse_filing_documents(indexed_soups, restrictions = ['year_of_incorp=2020', 'investment_group=pooled_investment_fund, no other'])

#finally save: on other computers will need to change the path!
path = "C:\\Users\\ASDAVY\\Documents\\Data Pageant Media\\"
if os.path.exists(path) :
    Extractosaur.df.to_csv(path + "data_for_pageant_media_ashley_davy.csv")

print(Extractosaur.df)