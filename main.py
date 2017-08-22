from urllib import *
from urllib.parse import urlparse, urljoin
from urllib.request import urlopen
import urllib.request
import requests
from bs4 import BeautifulSoup
import re, csv, threading, time


def download(url, num_retries=5):
    """Download function that also retries 5XX errors"""

    try:
        html = urlopen(url).read()
        # print(url, 'Success')
    except urllib.error.URLError as e:
        # print('Download error:', e.reason)
        html = None
        if num_retries > 0:
            if hasattr(e, 'code') and 500 <= e.code < 600:
                # print(str(e))
                # retry 5XX HTTP errors
                time.sleep(2)
                html = download(url, num_retries - 1)
            elif e.code == 404:
                # print(str(e))
                print('Page end')
                return html
        else:
            print(url, 'Failed')
    return html


def takeFirst(elem):
    return elem[0]


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

        self.output = open('result.csv', 'w', encoding='utf-8', newline='')
        self.writer = csv.writer(self.output)
        headers = ['No', 'Title', 'Rating', 'MPAA', 'Genre', 'Director', 'Stars', 'Votes', 'Gross', 'Release Date',
                   'Budget', 'Filming Location', 'Minutes', 'Language', 'Country', 'Keywords']
        self.writer.writerow(headers)

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
        self.output.close()

    def onepage_scraping(self):
        url = self.total_urls.pop()
        html = download(url)
        time.sleep(1)
        if html is None:
            self.scraping_done = True
            self.total_urls = []
        else:
            num = ''
            title = ''
            date = ''
            budget = ''
            filming_location = ''
            gross = ''
            rating = ''
            mpaa = ''
            genre = ''
            director = ''
            stars = ''
            votes = ''
            runtime = ''
            language = ''
            country = ''
            keywords = ''

            try:
                soup = BeautifulSoup(html, 'html.parser')
                time.sleep(1)
                table = soup.find("div", class_="lister-list")
                trs = table.find_all("div", class_="lister-item-content")
                for i, row in enumerate(trs):
                    # try:
                    h3 = row.find("h3", class_="lister-item-header")
                    title_line = h3.text.strip()
                    title_line = title_line.split('\n')
                    num = title_line[0].strip('.')
                    title = title_line[1].strip()

                    try:
                        link = h3.find("a")['href']
                        link = 'http://www.imdb.com' + link
                        sub_html = download(link)
                        time.sleep(1)
                        sub_soup = BeautifulSoup(sub_html, 'html.parser')
                        time.sleep(1)
                        title_bar = sub_soup.find("div", class_='title_wrapper')
                        date = title_bar.find_all('a')[-1].text.strip()

                        detail = sub_soup.find("div", id="titleDetails")
                        lines = detail.find_all("div", class_="txt-block")

                        base_link = link.split('?')[0]

                        for line in lines:
                            if 'Filming Locations:' in line.text:
                                try:
                                    location_link = base_link + line.find('span', class_='see-more').find('a')['href']
                                    sub_sub_html = download(location_link)
                                    time.sleep(1)
                                    sub_sub_soup = BeautifulSoup(sub_sub_html, 'html.parser')
                                    time.sleep(1)
                                    soda = sub_sub_soup.find_all('div', class_='soda')
                                    filming_location = []
                                    for s in soda:
                                        filming_location.append(s.find('dt').text.strip())
                                    filming_location = ' | '.join(filming_location)
                                except:
                                    filming_location = line.text.replace('Filming Locations:', '').strip()
                            if 'Budget:' in line.text:
                                budget = line.text.replace('Budget:', '').strip().split('\n')[0].strip()
                            if 'Gross:' in line.text:
                                gross_line = line.text.replace('Gross:', '').split('\n')
                                gross_line.remove('')
                                gross = []
                                for g in gross_line:
                                    gross.append(g.strip())
                                gross = ' '.join(gross)
                            if 'Language:' in line.text:
                                language = line.text.replace('Language:', '').strip().split('\n')[0].strip()
                            if 'Country:' in line.text:
                                country = line.text.replace('Country:', '').strip().split('\n')[0].strip()

                    except:
                        date = ''
                        budget = ''
                        filming_location = ''
                        gross = ''
                        runtime = ''
                        language = ''
                        country = ''

                    try:
                        tmp = []
                        keywords_line = sub_soup.find('div', itemprop = 'keywords').text.split('\n')
                        for elm in keywords_line:
                           if elm is '' or 'Plot Keywords:' in elm or '|'in elm:
                               continue
                           else:
                               tmp.append(elm.strip())
                        keywords = ' | '.join(tmp)

                    except:
                        keywords = ''
                    try:
                        genre = row.find("span", class_="genre").text.strip()
                    except:
                        genre = ''
                    try:
                        rating = row.find("div", class_="ratings-imdb-rating").text.strip()
                    except:
                        rating = ''
                    try:
                        mpaa = row.find("span", class_="certificate").text.strip()
                    except:
                        mpaa = ''
                    try:
                        runtime = row.find("span", class_="runtime").text.strip()
                    except:
                        runtime = ''
                    try:
                        votes_line = row.find("p", class_="sort-num_votes-visible").text
                        votes_line = votes_line.replace('\n', '').split('|')
                        try:
                            votes = re.search('Votes:(.*)', votes_line[0]).group(1).strip()
                        except:
                            votes = ''

                    except:
                        votes = ''

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

                    logTxt = "No:\t\t\t{0}\nTitle:\t\t{1}\nRating:\t\t{2}\nMPAA:\t\t{3}\nGenre:\t\t{4}\nDirector:\t{5}\n" \
                             "Stars:\t\t{6}\nVotes:\t\t{7}\nGross:\t\t{8}\nDate:\t\t{9}\nBudget:\t\t{10}\nLocation:\t{11}\n" \
                             "Minutes:\t{12}\nLanguage:\t{13}\nCountry:\t{14}\nKeywords:\t{15}\n". \
                        format(num, title, rating, mpaa, genre, director, stars, votes, gross, date, budget,
                               filming_location,
                               runtime, language, country, keywords)

                    print(logTxt)
                    self.total_data.append([
                        num, title, rating, mpaa, genre, director, stars, votes, gross, date, budget, filming_location,
                        runtime, language, country, keywords
                    ])
                    self.writer.writerow([
                        num, title, rating, mpaa, genre, director, stars, votes, gross, date, budget, filming_location,
                        runtime, language, country, keywords
                    ])
            except:
                self.total_urls = []

    def save_csv(self):
        print(
            "\n=====================================================================================================================================================================\n")
        print('Saving data into CSV')
        output = open('result.csv', 'w', encoding='utf-8', newline='')
        self.writer = csv.writer(output)
        headers = ['No', 'Title', 'Rating', 'MPAA', 'Genre', 'Director', 'Stars', 'Votes', 'Gross', 'Release Date',
                   'Budget', 'Filming Location', 'Minutes', 'Language', 'Country', 'Keywords']
        self.writer.writerow(headers)

        self.total_data.sort(key=takeFirst)
        for i, row in enumerate(self.total_data):
            self.writer.writerow(row)
        output.close()


if __name__ == '__main__':
    start_time = time.time()
    app = main_scraper()
    app.url_generation()
    app.start_scraping()
    # app.onepage_scraping()
    # app.save_csv()

    elapsed_time = time.time() - start_time
    print(elapsed_time)
