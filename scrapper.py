import re
import time

from selenium import webdriver
from rethinkdb import RethinkDB
import base64
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from selenium.webdriver.chrome.options import Options

urlMatch = r'https?:\/\/(www\.)?([-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}|localhost)\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)'
urlNoProto = r'\/\/(www\.)?([-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}|localhost)\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)'
urlPrefixSlash = r'((\/[a-zA-Z-]{0,100}){0,100})([\/\w])'
urlPrefixNoSlash = r'(([a-zA-Z-]{0,100}){0,100})([\/\w])'

r = RethinkDB()

r.connect("192.168.1.25", 28015).repl()

cursor = r.db('scratcher').table('tasks').filter(r.row["status"] == "PENDING").changes().run()

DRIVER_PATH = 'C:\PathFiles\chromedriver'
driver = webdriver.Chrome(executable_path=DRIVER_PATH)

for document in cursor:

    if(document['new_val'] == None):
        continue

    documentId = document['new_val']['id']
    documentUrl = document['new_val']['URL']

    if 'parentJob' in document['new_val']:
        parentJob = document['new_val']['parentJob']
    else:
        parentJob = document['new_val']['id']

    documentSearchDepth = int(document['new_val']['searchDepth'])

    print(documentId, documentUrl)

    options = Options()
    options.headless = True
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless")
    options.add_argument("headless")
    options.add_argument("--disable-gpu")

    try:
        driver.get(documentUrl)
    except:
        r.db('scratcher').table('tasks').get(documentId).update({'status': 'ERRORED'}).run()
        print("Skipped")
        continue

    parsedUrl = urlparse(driver.current_url)

    time.sleep(2)

    driver.save_screenshot('page.png')

    documentImageBytes = open('page.png', 'rb').read()
    documentImage = base64.b64encode(documentImageBytes).decode('utf-8')
    soup = BeautifulSoup(driver.page_source)

    if int(documentSearchDepth) > 0:
        for link in soup.find_all('a', href=True):
            print("Chcking", link)
            # We need to check link validity
            href = link['href']
            if re.match(urlMatch, href):
                hrefMapped = href
            elif re.match(urlNoProto, href):
                hrefMapped = f'https:{href}'
            elif re.match(urlPrefixSlash, href):
                hrefMapped = f'{parsedUrl.scheme}://{parsedUrl.netloc}{href}'
            elif re.match(urlPrefixNoSlash, href):
                hrefMapped = f'{parsedUrl.scheme}://{parsedUrl.netloc}/{href}'
            # Process the URL here.
            try:
                print(hrefMapped)
                parseUrl = urlparse(hrefMapped)
                newUrl = parseUrl.scheme + "://" + parseUrl.netloc + parseUrl.path
                if not r.db('scratcher').table('tasks').filter({ 'parentJob': parentJob, 'URL': newUrl}).count().run() > 0:
                    documentToInsert = {'URL': newUrl, 'status': 'PENDING',
                                        'searchDepth': documentSearchDepth - 1, 'parentJob': parentJob}
                    r.db('scratcher').table('tasks').insert(documentToInsert).run()
            except:
                print("Skipping URL: %{f}, unable to parse url")
                print("Skipping", link['href'])

    documentToEdit = {'pageTitle': driver.title,'finalUrl': driver.current_url, 'status': "COMPLETE"}

    documentDump = {'imageBlob': f'data:image/{"png"};base64,{documentImage}', 'pageTitle': driver.title,
                      'pageSource': soup.prettify(),
                      'finalUrl': driver.current_url, 'status': "COMPLETE", "crossRef": documentId}


    r.db('scratcher').table('results').insert(documentDump).run()
    r.db('scratcher').table('tasks').get(documentId).update(documentToEdit).run()
