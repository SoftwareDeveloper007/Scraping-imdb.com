from urllib import *
from urllib.parse import urlparse, urljoin
from urllib.request import urlopen
import urllib.request
import requests
from bs4 import BeautifulSoup
import re
import csv
import threading
import time


def download(url, num_retries=5):
    """Download function that also retries 5XX errors"""

    try:
        html = urlopen(url).read()
        #print(url, 'Success')
    except urllib.error.URLError as e:
        #print('Download error:', e.reason)
        html = None
        if num_retries > 0:
            if hasattr(e, 'code') and 500 <= e.code < 600:
                # retry 5XX HTTP errors
                time.sleep(2)
                html = download_html(url, num_retries - 1)
            elif e.code == 404:
                print('Page end')
                return html
        else:
            print(url, 'Failed')
    return html


class main_scraper():
    def __init__(self):
        base_url = 'http://www.imdb.com/search/title?certificates=us%3Ag,us%3Apg,us%3Apg_13,us%3Ar,us%3Anc_17'
        self.config = open('Config.txt', 'r').read().split('\n')

        production_status = ''
        release_date = ''
        release_from = ''
        release_to = ''
        title_type = ''

        for row in self.config:
            if 'Production status' in row:
                production_status = row.split('=')[1].strip()
            elif 'Release date' in row:
                release_date = row.split('=')[1].strip()
                release_from = release_date.split('~')[0].strip()
                release_to = release_date.split('~')[1].strip()
            elif 'Title type' in row:
                title_type = row.split('=')[1].strip()

        self.start_url = base_url

        if production_status is not '':
            self.start_url = self.start_url + '&production_status={}'.format(production_status)
        if release_from is not '' and release_to is not '':
            self.start_url = self.start_url + '&release_date={},{}'.format(release_from, release_to)
        if title_type is not '':
            self.start_url = self.start_url + '&title_type={}'.format(title_type)

        self.total_urls = []
        self.total_data = []
        self.scraping_done = False
        self.cnt = 0

    def url_generation(self):

        for i in range(1000):
            self.total_urls.append(self.start_url + '&page={}&ref_=adv_nxt'.format(i + 1))

        self.total_urls.reverse()

    def start_scraping(self):
        self.threads = []
        self.max_threads = 10

        print('\nScraping started!!!\n')

        while self.threads or self.total_urls:
            for thread in self.threads:
                if not thread.is_alive():
                    self.threads.remove(thread)

            while len(self.threads) < self.max_threads and self.total_urls:
                thread = threading.Thread(target=self.onepage_scraping)
                thread.setDaemon(True)
                thread.start()
                self.threads.append(thread)

        print('\nScraping completed Successfully!!!')

    def onepage_scraping(self):
        url = self.total_urls.pop()
        html = download(url)
        if html is None:
            self.scraping_done = True
            self.total_urls = []
        else:
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find("div", class_="lister-list")
            trs = table.find_all("div", class_="lister-item-content")
            for i, row in enumerate(trs):
                try:
                    title_line = row.find("h3", class_="lister-item-header").text.strip()
                    title_line = title_line.split('\n')
                    num = title_line[0].strip('.')
                    title = title_line[1].strip()
                    date = title_line[2]
                except:
                    num = ''
                    title = ''
                    date = ''
                try:
                    certificate = row.find("span", class_="certificate").text.strip()
                except:
                    certificate = ''
                try:
                    runtime = row.find("span", class_="runtime").text.strip()
                except:
                    runtime = ''
                try:
                    genre = row.find("span", class_="genre").text.strip()
                except:
                    genre = ''
                try:
                    rating = row.find("div", class_="ratings-imdb-rating").text.strip()
                except:
                    rating = ''
                try:
                    ps = row.find_all("p")
                    for j, p in enumerate(ps):
                        director_line = p.text.replace('\n', '').strip()
                        if 'Director:' in director_line:
                            try:
                                # director = re.search('Director:(.*)|', director_line).group(1).strip()
                                director = re.search('Director:(.*)', director_line.split('|')[0]).group(1).strip()
                            except:
                                director = ''
                        elif 'Directors:' in director_line:
                            try:
                                director = re.search('Directors:(.*)', director_line.split('|')[0]).group(1).strip()
                            except:
                                director = ''
                        if 'Stars:' in director_line:
                            stars = re.search('Stars:(.*)', director_line.split('|')[1]).group(1).strip()
                except:
                    director = ''
                    stars = ''
                try:
                    votes_line = row.find("p", class_="sort-num_votes-visible").text
                    votes_line = votes_line.replace('\n', '').split('|')
                    try:
                        votes = re.search('Votes:(.*)', votes_line[0]).group(1).strip()
                    except:
                        votes = ''
                    try:
                        gross = re.search('Gross:(.*)', votes_line[1]).group(1).strip()
                    except:
                        gross = ''
                except:
                    votes = ''
                    gross = ''

                self.cnt += 1
                txt = "--------------------------------------------------------------------------------------------------------------------" + \
                      "------------------------------------------------------------------------------------------\n" + \
                      "No | Title | Date | Certificate | Runtime | Genre | Rating | Director | Stars | Votes | Gross\n" + \
                      "{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10}\n{11}\n".format(num, title, date, certificate, runtime, genre, rating, director, stars, votes, gross, self.cnt)
                print(txt)


                #self.writer.writerow([num, title, date, certificate, runtime, genre, rating, director, stars, votes, gross])

                self.total_data.append({
                    'no': num,
                    'title': title,
                    'date': date,
                    'certificate': certificate,
                    'runtime': runtime,
                    'genre': genre,
                    'rating': rating,
                    'director': director,
                    'stars': stars,
                    'votes': votes,
                    'gross': gross
                })


    def save_csv(self):
        print("\n=====================================================================================================================================================================\n")
        print('Saving data into CSV')
        output = open('result.csv', 'w', encoding='utf-8', newline='')
        self.writer = csv.writer(output)
        headers = ['No', 'Title', 'Release Date', 'Certificate', 'Runtime', 'Genre', 'Rating', 'Director', 'Stars', 'Votes',
                   'Gross']
        self.writer.writerow(headers)

        for i, row in enumerate(self.total_data):
            self.writer.writerow([row['no'], row['title'], row['date'], row['certificate'], row['runtime'], row['genre'],
                                  row['rating'], row['director'], row['stars'], row['votes'], row['gross']])
        output.close()


if __name__ == '__main__':
    start_time = time.time()
    app = main_scraper()
    app.url_generation()
    app.start_scraping()
    # app.onepage_scraping()
    app.save_csv()

    elapsed_time = time.time() - start_time
