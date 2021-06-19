# -*- coding: utf-8 -*-

# Import libraries
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta

# This is where we record our data
data = [ [] for i in range(8) ] # 0:Title, 1: Date posted, 2:Company, 3:Rating, 4:Location, 5:Salary, 6:description, 7:Link

# Record the time the search initiated and put the output time in the file name
now = datetime.now() 
now_str = now.strftime("%Y-%m-%d_%H-%M-%S")
out = ''.join(['jobs_', now_str, '.ftr'])

#This is the base url for searches
search_base = 'https://www.indeed.com/jobs?q=data+science&limit=50&start='

count = 1
breaks = 0
while True:
    print('Page: ', count)
    print('Scrapes: ', len(data[0]), '\n' )
    
    # Perform search and create a soup object
    search_url = ''.join([search_base,str( len(data[0]) )])
    html = Request(search_url, headers={'User-Agent': 'Mozilla/5.0'})
    time.sleep(3) # Allow 3 seconds for the search to occur
    page = urlopen(html)
    content = page.read()
    soup = BeautifulSoup(content, 'html.parser')
    
    # Locate job cards
    flag = 0 # 0: No popup, 1: Popup
    cards = soup.findAll('div', attrs={'class': 'jobsearch-SerpJobCard unifiedRow row result'})
    if len(cards) == 0: # If job cards cannot be found, there may be a popup, which changes some of our search parameters
        cards = soup.select("a[class*='sponsoredJob resultWithShelf sponTapItem tapItem-noPadding desktop']") #Partial text
        flag = 1
    
    if len(cards) > 0:
        
        # Here we go through each job card, pulling the relevant data
        for card in cards:
            
            # Get job title
            if flag == 0:
                pull = card.findAll('a', attrs={'class': 'jobtitle turnstileLink'})
                try:
                    data[0].append(pull[0]['title'])
                except:
                    data[0].append('None')
            else:
                pull = card.findAll('div', attrs={'class': 'heading4 color-text-primary singleLineTitle tapItem-gutter'})
                try:
                    data[0].append(pull[0].text.strip())
                except:
                    data[0].append('None')
                
            # Get date of posting
            if flag == 0:
                pull = card.findAll('span', attrs={'class': 'date date-a11y'})
            else:
                pull = card.findAll('span', attrs={'class': 'date'})
            try:
                relative_date = pull[0].text.strip().lower()
                today = datetime.now()
                if 'just' in relative_date or 'today' in relative_date:
                    data[1].append( today.strftime("%Y-%m-%d") )
                else:
                    if 'active' in relative_date:
                        relative_date = relative_date[7:]
                    abs_date = today-timedelta( days=int( relative_date[:2] ) )
                    data[1].append( abs_date.strftime("%Y-%m-%d") )
                    if '+' in relative_date:
                        data[1][-1] += '+'
            except:
                data[1].append('None')
            
            # Get company name
            pull = card.findAll('a', attrs={'data-tn-element': 'companyName'})
            try:
                data[2].append( pull[0].text.strip() )
            except:
                pull = card.findAll('span', attrs={'class': 'company'})
                try:
                    data[2].append( pull[0].text.strip() )
                except:
                    pull = card.findAll('span', attrs={'class': 'companyName'})
                    try:
                        data[2].append( pull[0].text.strip() )
                    except:
                        data[2].append('None')

            # Get rating
            if flag == 0:
                pull = card.findAll('span', attrs={'class': 'ratingsContent'})
            else:
                pull = card.findAll('a', attrs={'class': 'ratingLink'})
            try:
                data[3].append( pull[0].text.strip() )
            except:
                data[3].append('None')
            
            # Get location
            if flag == 0:
                pull = card.findAll('span', attrs={'class': 'location accessible-contrast-color-location'})
                try:
                    data[4].append( pull[0].text.strip() )
                except:
                    pull = card.findAll('div', attrs={'class': 'location accessible-contrast-color-location'})
                    try:
                        data[4].append( pull[0].text.strip() )
                    except:
                        data[4].append('None')
            else:
                pull = card.findAll('div', attrs={'class': 'companyLocation'})
                try:
                    data[4].append( pull[0].text.strip() )
                except:
                    pull = card.findAll('span', attrs={'class': 'companyLocation'})
                    try:
                        data[4].append( pull[0].text.strip() )
                    except:
                        data[4].append('None')
            
            # Get salary 
            if flag == 0:
                pull = card.findAll('span', attrs={'class': 'salaryText'})
            else:
                pull = card.findAll('span', attrs={'class': 'salary-snippet'})
            try:
                data[5].append( pull[0].text.strip() )
            except:
                data[5].append('None')
              
            # Get description
            job_url = ''.join(['https://www.indeed.com/viewjob?jk=',card['data-jk']])
            job_html = Request(job_url,headers={'User-Agent': 'Mozilla/5.0'})
            time.sleep(3)
            job_page = urlopen(job_html).read()
            job_soup = BeautifulSoup(job_page, 'html.parser')
            pull = job_soup.findAll('div', attrs={'class': 'jobsearch-jobDescriptionText'})
            try:
                text = pull[0].text.strip()
                data[6].append( text.replace('\n',' ') )
            except:
                data[6].append('None')
            
            # Get url
            data[7].append(job_url)
            
            # Pause for 30 seconds between each description scrape
            time.sleep(30)
            
        count += 1
        
    # After each search, put results in a dataframe and save them to a feather file
    if len(data[0]) > 0:
        df=pd.DataFrame()
        df['Title']=data[0]
        df['Posted']=data[1]
        df['Company']=data[2]
        df['Rating']=data[3]
        df['Location']=data[4]
        df['Salary']=data[5]
        df['Description']=data[6]
        df['Link']=data[7]
        df.to_feather(out)
        del(df)

    # If our search failed (i.e., no job cards were found on the page), we probably hit a captcha page. We wait 5 minutes before trying again.
    if len(cards) == 0:
        breaks += 1
        print('Could not pull. Waiting 5 minute to try again.')
        print('Brakes taken: ',breaks,'\n')
        time.sleep(300)
        
