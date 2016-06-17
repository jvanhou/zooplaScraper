# -*- coding: utf-8 -*-
"""
@author: jvanhouten

PYTHON 3.5

if you want to update an existing file, 
make sure you update 'fileNameWithoutExt' accordingly 
and place this file in the folder of the python script

"""
############################################
# DEPENDENCIES
############################################
#import urllib
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import datetime as dt
from time import strptime
import re
import numpy as np

# for multi-threading
from multiprocessing.dummy import Pool as ThreadPool

############################################
# USER INPUTS
############################################
# URLs (replace with url from custom search)
#sOVERVIEW_URL = r'http://www.zoopla.co.uk/for-sale/property/london/?include_retirement_homes=true&include_shared_ownership=true&new_homes=include&price_max=3000000&price_min=250000&q=London&results_sort=newest_listings&search_source=for-sale'
sOVERVIEW_URL = r'http://www.zoopla.co.uk/for-sale/property/london/?include_retirement_homes=true&include_shared_ownership=true&new_homes=include&price_max=1000000&price_min=350000&q=London&results_sort=newest_listings&search_source=home'

# pages to scrape
iMAX_PAGES = 80 # max 400 # change based on number of pages to scrape

# filenames
fileNameWithoutExt = 'ZooplaScrape'

############################################
# CONSTANTS
############################################
# html identifiers
sPROPERTY_URL = r'http://www.zoopla.co.uk/for-sale/details/IDENTIFIER'
sCLASS_SECTION = 'sidebar sbt'
sCLASS_TOP = 'top'
sSB_NAME = 'Listing history'
sFIRST_LISTED = 'First listed'
sCHANGES = 'most_reduced_list'
sIDENTIFIER = 'IDENTIFIER'
sPAGE_NUM_PREFIX = '&pn='

# table headers
iDColName = 'Identifier'
originalDateColName = 'First listed date'
dateColName = 'Changes date'
originalPriceCol = 'Original price'
newPriceCol = 'New price'
beds = 'Beds'
propertyType = 'Type'
postCode = 'Post code'
address = 'Address'
latitude = 'Latitude'
longitude = 'Longitude'
urlColName = 'URL'
headers = [iDColName, originalDateColName, dateColName, originalPriceCol, newPriceCol, 
           beds, propertyType, postCode, address, latitude, longitude, urlColName]

############################################
# VARIABLES
############################################
# create dictionary with mapping of tabnames and list of available datapoints
dictTabHeaders = {'foo':'bar'}

############################################
# CLASSES
############################################

class ZooplaSite:
    """
    can return a list of links to individual houses
    and when at end of list click next page to produce next list etc
    """    
    def __init__(self, overviewURL, maxPagesToView):
        self.url = overviewURL
        self.data = dataset(True, fileNameWithoutExt + '.xlsx')
        self.pages = [] # list of overview pages given search (1 to N)
        self.maxPages = maxPagesToView
        
    def assemble_link(self, identifier):
        url = sPROPERTY_URL
        url = url.replace(sIDENTIFIER, identifier)
        return url

    def load_website(self):
        """
        loads a url into a bs soup object
        """
#        r = urllib.request.urlopen(self.url).read()
        r = requests.get(self.url).content        
        self.soup = bs(r, "lxml")
    
    def find_number(self, string):
        """
        strip numbers from string
        """
        #string = string.encode('ascii', 'ignore')
        #return int(filter(str.isdigit, string))
        s = (re.findall('\d+', string))
        return int(''.join(s))
        
    def get_overview_pages(self):
        """
        gets a list of 1 to N overview pages
        """
        self.load_website()
        maxNumber = 1
        for pageIndex in self.soup.find_all('div', {'class':'paginate bg-muted'}):
            for link in pageIndex.find_all('a'):
                # try to convert string to number; if error it's not a number
                try:
                    number = int(link.text)
                    if number > maxNumber:
                        maxNumber = number                    
                except ValueError:
                    pass
        print('Screening complete: %d pages found - accessing first %s pages' % (maxNumber, self.maxPages))
        self.pages = [np.arange(1, maxNumber, 1)]

    def get_listings_on_page(self, url):
        self.url = url
        self.load_website()
        listOfIDs = []
        for listings in self.soup.find_all('ul', {'class':'listing-results clearfix'}):
            for listing in listings.find_all('li'):
                try:
                    listOfIDs.append(listing.attrs['data-listing-id'])
                except KeyError:
                    # not a property listing
                    pass
        return listOfIDs
        
    def load_properties(self):
        """
        get list of all potential page numbers
        for each page number, get list of properties
        for each property, load propertypage and store data
        """
        self.get_overview_pages()
        # loop over page numbers to get list of property IDs
        for pageNum in np.nditer(self.pages):
            if pageNum <= self.maxPages:
                overviewURL = sOVERVIEW_URL + sPAGE_NUM_PREFIX + str(pageNum)
                print('Accessing: %s' % overviewURL)
                listings = self.get_listings_on_page(overviewURL)
                # now loop over all listings on page, and load url
                for i in range(len(listings)):
                    print('viewing property: %s' % listings[i])
                    url = self.assemble_link(listings[i])
                    # set up property page and load data
                    aProperty = PropertyPage(url)
                    aProperty.load_website()
                    aProperty.load_data(listings[i])
                    self.data.add_data(aProperty.df)
               
    def load_property(self, listing):
        """
        for use in multi-threading
        loads property page and stores data in df
        returns df
        """
        print('viewing property: %s' % listing)
        url = self.assemble_link(listing)
        # set up property page and load data
        aProperty = PropertyPage(url)
        aProperty.load_website()
        aProperty.load_data(listing)
        return aProperty.df                                 

    def load_properties_async(self, numberOfThreads):
        """
        view multiple properties at a time
        """
        self.get_overview_pages()
        # loop over page numbers to get list of property IDs
        for pageNum in np.nditer(self.pages):
            if pageNum <= self.maxPages:
                overviewURL = sOVERVIEW_URL + sPAGE_NUM_PREFIX + str(pageNum)
                print('Accessing: %s' % overviewURL)
                listings = self.get_listings_on_page(overviewURL)  
                pool = ThreadPool(numberOfThreads)
                listings = np.asarray(listings)
                results = pool.map(self.load_property, listings)
                for result in results:
                    self.data.add_data(result)
                pool.close()
                
    def save_data(self, fileName):
        self.data.write_to_excel(fileName)


class PropertyPage(ZooplaSite):
    """
    loads website and returns information from the table with electricity data
    """
    def __init__(self, url):
        self.url = url
        self.result = pd.DataFrame() # dataframe should store: url, first listed date+price and changes to price
        self.df = pd.DataFrame(columns=headers)

    def get_date(self, string):
        """
        strips a datetime object out of a string date (## - Mon - ####)
        """
        # remove new lines
        string = string.replace('\n', '')
        # first, get first digit - day is then number value of following 2 chars        
        firstDigit = re.search('\d', string)
        day = string[firstDigit.start():firstDigit.start()+2]
        day = self.find_number(day)
        # then get year - match 4 digits
        yearLoc = re.search(r'\d{4}(?!\d)', string)
        year = string[yearLoc.start():yearLoc.end()]
        # then get month
        monthLoc = re.search(r'[A-Z]{1}[a-z]{2}', string)
        month = string[monthLoc.start():monthLoc.end()]
        try:
            month = strptime(month, '%b').tm_mon
            date = dt.datetime(int(str(year)), int(str(month)), int(str(day)))
        except ValueError:
            pass
            date = np.NAN
        return date
        
    def load_data(self, identifier):
        """
        load data for all countries (if args is nothing)
        or for tuple(country, year) combination
        """
        propertyType = self.soup.find('h2', class_='listing-details-h1').text
        propertyType = propertyType.replace(' for sale', '')
        beds = re.findall(r'\d{1} bed', propertyType)
        if beds == []:
            # get first word
            beds = '1 bed'
        else:
            beds = beds[0]
        # get general info of property
        propertyType = propertyType.replace(str(beds) + ' ', '')
        address = self.soup.find('h2', {'itemprop':'streetAddress'}).text      
        postCode = address.rsplit(None, 1)[-1]
        address = address.replace(postCode, '')
        latLong = self.soup.find('meta', {'itemprop':'latitude'}).attrs
        latitude = latLong['content']
        latLong = self.soup.find('meta', {'itemprop':'longitude'}).attrs
        longitude = latLong['content']
        # original price and changes to price are in sidebar
        for sidebar in self.soup.find_all('div', {"class":sCLASS_SECTION}):
            count = 0
            # price info in sidebar called 'Listing history'                        
            if sidebar.find(text=sSB_NAME):
                try:
                    originalPriceAndDate = sidebar.find(text=sFIRST_LISTED).next_element.next_element
                    originalPrice = originalPriceAndDate[:originalPriceAndDate.find(' on')]
                    originalPrice = self.find_number(originalPrice)
                    originalDate = originalPriceAndDate[originalPriceAndDate.find(' on') + 3:]
                    originalDate = self.get_date(originalDate)
                    # store original listing - in same order as headers!
                    result = [identifier + "_" + str(count), originalDate, np.NaN, originalPrice, np.NaN, 
                              beds, propertyType, postCode, address, latitude, longitude, self.url]
                    series = pd.Series(result, name=identifier + "_" + str(count), index=headers)
#                    self.df = self.df.append(pd.Series(result, index=headers), ignore_index=True)  
#                    self.df = self.df.append(series) 
#                    self.df = pd.concat([self.df, series])
                    self.df = self.df.append(series, ignore_index=False)
                except (AttributeError, UnboundLocalError):
                    print('Error viewing this property')
                    pass
                try:
                    # store any changes to original listing
                    for changes in sidebar.find_all('ul', {'class':sCHANGES}):
                        for change in changes.find_all('li'):
                            count += 1
                            # get date and new price
                            date = change.find('span').text
                            date = date.replace('Reduced on:', '')
                            date = date.replace('\n', '')
                            date = self.get_date(date)
                            newPrice = self.find_number(change.next_element)
                            # store result in order of headers
                            result = [identifier + "_" + str(count), originalDate, date, originalPrice, newPrice, 
                                      beds, propertyType, postCode, address, latitude, longitude, self.url]
                            series = pd.Series(result, name=identifier + "_" + str(count), index=headers)                           
#                            self.df = self.df.append(pd.Series(result, index=headers), ignore_index=True)
                            self.df = self.df.append(series, ignore_index=False)
#                            self.df = pd.concat([self.df, series])
                except (AttributeError, UnboundLocalError):
                    print('No changes')
                    pass
                        
class dataset:
    """
    stores dataframes for different countries
    """
    def __init__(self, loadExisting=True, fileName=""):
        if loadExisting:
            try:
                print('Loaded %s' % fileName)
                self.data = pd.read_excel(fileName)
            except FileNotFoundError:
                print('%s not found' % fileName)
                self.data = pd.DataFrame()
        else: self.data = pd.DataFrame()

    def add_data(self, df):
        """
        adds dataframe to result df
        """
        # TODO: improve merging code
        self.data = self.data.append(df, ignore_index=False)
        self.data = self.data[~self.data.index.duplicated(keep='first')]
#        self.data = pd.concat([self.data, df])
    
    def write_to_excel(self, fileNameNoExtension):
        """
        takes the result df and writes it to Excel
        """
        self.data.to_excel(fileNameNoExtension + '.xlsx', engine='xlsxwriter')

############################################
# MAIN FUNCTIONS
############################################

def main():
    zoopla = ZooplaSite(sOVERVIEW_URL, iMAX_PAGES)
#    zoopla.load_properties()
    zoopla.load_properties_async(25)
    zoopla.save_data(fileNameWithoutExt)

if __name__ == "__main__":
    main()
