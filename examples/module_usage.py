import requests
import time
import json
from foolcalls.scrapers import scrape_transcript_urls_by_page, scrape_transcript

# get one transcript for a known url
transcript_url = 'https://www.fool.com/earnings/call-transcripts/2020/07/15/kura-sushi-usa-inc-krus-q3-2020-earnings-call-tran.aspx'
response = requests.get(transcript_url)
transcript = scrape_transcript(response.text)
print(json.dumps(transcript, indent=2))

# get all (~20) transcript urls from a single page
transcript_urls = scrape_transcript_urls_by_page(page_num=1)
print(transcript_urls)

# combine the two above to get a handful of transcripts from fool.com
num_of_pages = 3 # will be ~60 transcripts, with ~20 transcript urls per page
output = []
for page in range(0, num_of_pages):
    transcript_urls = scrape_transcript_urls_by_page(page)
    for transcript_url in transcript_urls:
        response = requests.get(transcript_url)
        transcript = scrape_transcript(response.text)
        output.append(transcript)
        time.sleep(5) # sleep 5 seconds between reqeusts
print(output)