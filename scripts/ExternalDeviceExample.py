#Proof of concept ES Correlation Script
#Locates all usb devices returned from tzworks usp script
#Determines unique volume serial numbers
#Writes xlsx with one tab per volume serial number of all known entries
# Copyright 2015 G-C Partners, LLC
# David Cowen
#
# G-C Partners licenses this file to you under the Apache License, Version
# 2.0 (the "License"); you may not use this file except in compliance with the
# License.  You may obtain a copy of the License at:
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

#Json is here to help in debugging by printing dumps of structures
import json
import sys
#First we need to bring in our libraries, starting with elasticsearch
import elasticsearch
#sys and codecs to access functions and map the windows unicode encodings in such a way that python will handle them
import codecs
#xlsxwriter best xlsx writer
import xlsxwriter
#We can use the search class built in elasichandler so we don't have to worry about the
#size of our result sets
sys.path.append('..')
import elastichandler

#Define some variables for elastic's connection here
HOST = '127.0.0.1'
INDEX = 'case_index'

#Create Elastic configuration for elastichandler
esConfig = elastichandler.EsConfig(
    HOST
)

#Create Elastic Handler object from elastichandler passing in the config object
esHandler = elastichandler.EsHandler(
    esConfig
)

#Set the current index of the handler for the elastichandler object to query
esHandler.SetCurrentIndex(
    INDEX
)

#Dealing with windows unicode wackiness
codecs.register(lambda name: codecs.lookup('utf-8') if name == 'cp65001' else None)
#Defining the name of the xlsx file we will be writing, I'm hardcoding it here
workbook = xlsxwriter.Workbook('output.xlsx')
#Creating the first worksheet or tab in the xlsx spreadsheet
w=workbook.add_worksheet('USB Devices')
#Creating our elasticsearch object
es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200

#This is taken from Ryan Benson's hindsight code, we are just setting the formatting
#of the strings we will write to our xlsx spreadsheet
black_type_format    = workbook.add_format({'font_color': 'black', 'align': 'left'})

#Writing a description to the first row of our worksheet of whats being shown there
w.write_string(0,0,"Listing of USB Storage devices plugged into system", black_type_format)
#Let's make sure we start on the second row so we don't overwrite our header
rownumber=1
'''Our first query against the loaded ElasticSearch data, pulling back all the usb devices
that the TzWorks USP tool parsed out. You can pass in json as I'm doing here or a dict as seen
in the example below. The advantage of using this function over a straight elastic search query
is that the eshandler function implements the scrolls object so it will iterate over all returned hits
for you and return it as a list rather than you having to do all of that yourself'''
for usb in esHandler.FetchRecordsFromQuery('{"query": {"term": {"_type": "tz_usp"}}}'):
    #If we are in the first row then we need to write out the headers of our returned data 
    if rownumber==1:
            columnnumber=0
            #Setting the variable usbd equal to the _source subkey where all the usb device data is returned
            usbd=usb['_source']
            for us in usbd:
                #Write each header to the 2nd line of the tab starting at the first column
                w.write_string(rownumber, columnnumber, str(us), black_type_format)
                #increment which column we will write to next
                columnnumber += 1
                #increment our rownumber 
            rownumber+=1
    #Reset our column number as now we are moving a row down
    columnnumber=0
    #Again redefining usbd
    usbd=usb['_source']
    #iterating through the results
    for us in usbd:
        #This time we are writing our the values stored within the results rather than the value names as before
        w.write_string(rownumber, columnnumber, str(usbd[us]), black_type_format)
        columnnumber += 1
    print ""
    rownumber += 1
#To make this nice and friendly we are going to freeze the first two lines so that they stay put while the users scrolls
#allowing them to see the headers at all times
w.freeze_panes(2, 0)               
#Let's add a filter to each column making it easy for the user
w.autofilter(1, 0, rownumber, 23)  

'''This elastic query is using another function called aggs or aggregates that returns the count of a certain thing.
In this case we are telling aggs to give us the list of all volume serial numbers and the count of how many records exist
for them. This works because our mappings from the elastichandler project have normalized the field names between all of our
reports so that all fields that contain Volume Serial numbers are now named Volume Serial'''
result = es.search(index=INDEX, body={"size": 0,"aggs": {"serials": { "terms" :{"field" : "Volume Serial"}}}})
#Next we store the sub list of the result that contains the data we want to work with to 'hits'
hits = result['aggregations']['serials']['buckets']
#Iterate through all the unique volume serial numbers
for hit in hits:
    #Store our volume serial number returned from the previous query as a string, removing the unicode tag
    volume=str(hit["key"])
    #Let's add a new tab/worksheet named after the volume serial number
    q=workbook.add_worksheet(volume)
    #Now we can write the description of the worksheet to the first line
    q.write_string(0,0,"Files known to exist on this volume from lnk files and jumplists",black_type_format)
    rownumber=1
    '''Next we need to build our query string for elastic. It contains a variable name so we can't just write it into the function this time
    So we are building a dictionary in python of the parameters that the FetchRecordsFromQuery function in eshandler supports.
    We are telling elastic search to run a query, use wildcards when searching around our volumename and finally the volume name itself
    '''
    query = {
        "query":{
            "query_string":{
                "analyze_wildcard": True,
                "query":volume
            }
        }
    }
    #We are then calling our query functin and passing in our dict query
    for record in esHandler.FetchRecordsFromQuery(query):
        #Same as before writing out the header row
        if rownumber==1:
            columnnumber=0
            #Again the data we want to work with is underneath the _source object 
            sources=record['_source']
            for source in sources:
                q.write_string(rownumber, columnnumber, str(source), black_type_format)
                columnnumber += 1
            rownumber += 1
        
        sources=record['_source']
        columnnumber=0
        for source in sources:
        
            q.write_string(rownumber, columnnumber, str(sources[source]), black_type_format)
            columnnumber += 1
        print ""
        rownumber += 1
    q.freeze_panes(2, 0)                
    q.autofilter(1, 0, rownumber, 36)
#Closing the xlsx file and exiting
workbook.close()
