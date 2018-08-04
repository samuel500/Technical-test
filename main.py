from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import threading
import json
import pandas as pd
from time import time


standards_url = "https://www.instituteforapprenticeships.org/apprenticeship-standards/"
apprenticeships_start_url = "https://findapprenticeshiptraining.sfa.bis.gov.uk/Apprenticeship/SearchResults?page="
apprenticeships_end_url = "&order=1"



def get_soup(url):
	"""Return soup object of web page."""
	uClient = uReq(url)
	page_html = uClient.read()
	uClient.close()
	page_soup = soup(page_html, "html.parser")
	return page_soup


def list_to_json_file(list_to_save, file_name):
	"""Save list of elements to new-line delimited JSON file."""
	txt = ""
	for element in list_to_save:
		txt += json.dumps(element) + "\n"

	with open(file_name, 'w') as f:
		f.write(txt)


def normalize_title(title):
	"""Remove elements of title that may interfere with matching the datasets. Reduces legibility."""
	title = title.strip().lower().replace(' ', '').replace(':', '').replace(',', '')
	return title

#step_1a

def scrape_standards(url):
	"""Return apprenticeship standards."""
	page_soup = get_soup(url)
	#classes = ['standard approved', 'standard inDevelopment', 'standard decommissioned']

	standards_soup = page_soup.findAll('div', {'class': 'standard approved'}) #Ignoring "In development" and "Decommissioned" standards.

	standards = []
	for standard in standards_soup:
		new_std = {}
		title = normalize_title(standard.h3.text)
		new_std['title'] = title
		level = standard.findAll('span', {'class': 'level'})[0].text.split(' ')[1]
		level = int(level)
		new_std['level'] = level
		duration = standard.findAll('span', {'class': 'duration'})[0].text.split(' ')[0]
		duration = int(duration)
		new_std['duration'] = duration

		max_funding = standard.findAll('span', {'class': 'funding'})[0].text.split(' ')[-1][1:]
		max_funding = int(max_funding)
		new_std['max_funding'] = max_funding

		standards.append(new_std)

	return standards



#step_1b

def get_nb_pages():
	"""Return number of pages in apprenticeship dataset."""
	url = apprenticeships_start_url + "1" + apprenticeships_end_url
	page_soup = get_soup(url)
	counter_str = page_soup.findAll('span', {'class': 'counter'})[0].contents[0]
	nb_pages = int(counter_str.split(" ")[-1])

	return nb_pages


def scrape_apprenticeship_pages(urls):
	"""Return the list of apprenticeships with title, level and length on the url pages."""
	apprenticeships = []
	for url in urls:
		page_soup = get_soup(url)
		apprenticeships_soup = page_soup.findAll('article')

		for apprenticeship in apprenticeships_soup:
			new_app = {}
			title = normalize_title(apprenticeship.findAll('a')[0].text)
			new_app['title'] = title
			info = apprenticeship.findAll('dd')
			level_info = info[0].text.strip()

			level = int(level_info[0])
			level_detail = level_info[1:].strip()
			new_app['level'] = level
			new_app['level-detail'] = level_detail
			duration = int(info[1].text.split(' ')[0]) #In months
			new_app['duration'] = duration
			apprenticeships.append(new_app)

	return apprenticeships


def scrape_apprenticeships(pages_per_thread = 3):
	"""Return all apprenticeships on site."""

	threads = []

	nb_pages = get_nb_pages()
	all_urls = [apprenticeships_start_url + str(i) + apprenticeships_end_url for i in range(1, nb_pages+1)]
	nb_threads = int((len(all_urls)-1)/(pages_per_thread))+1
	urls = [all_urls[i*pages_per_thread:i*pages_per_thread+pages_per_thread] for i in range(nb_threads)]

	for i in range(len(urls)):
		newThread = scrapingThread(i, urls[i])
		threads.append(newThread)

	for i in range(0, len(threads)):
		threads[i].start()

	for i in range(0, len(threads)): #Wait until threads finished executing
		threads[i].join()

	all_apprenticeships = []
	for thread in threads:
		all_apprenticeships += thread.apprenticeships

	return all_apprenticeships


class scrapingThread (threading.Thread):
	def __init__(self, thread_ID, urls):
		threading.Thread.__init__(self)
		self.thread_ID = thread_ID
		self.urls = urls
		self.apprenticeships = []
	def run(self):
		self.apprenticeships = scrape_apprenticeship_pages(self.urls)


def main():


	"""
	#Optimization test for pages_per_thread
	ppts =  [1, 2, 3, 4, 5, 6, 7, 10, 15, 20]
	for i in ppts:
		print("ppt:", i)
		nb_iter = 12
		t_tot = 0
		for y in range(nb_iter):
			tStart = time()
			apprenticeships = scrape_apprenticeships(pages_per_thread = i)
			t_tot += time()-tStart
		print("avg time:", t_tot/nb_iter)
	raise
	#1 page per thread is the fastest, 3 or 4 pages per thread probably optimal.
	"""

	standards = scrape_standards(standards_url)
	apprenticeships = scrape_apprenticeships()
	#print(len(standards))
	#print(len(apprenticeships))
	list_to_json_file(standards, "step_1a.json")
	list_to_json_file(apprenticeships, "step_1b.json")


	#step_2a
	df_standards = pd.DataFrame(standards)
	df_apprenticeships = pd.DataFrame(apprenticeships)

	dataset_match = pd.merge(df_standards, df_apprenticeships, on=['level', 'duration', 'title'], how='outer')
		# = len(pd.merge(df_standards, df_apprenticeships, on=['level', 'duration', 'title'], how='inner'))

	list_of_dataset = dataset_match.to_dict('records')

	list_to_json_file(list_of_dataset, "step_2a.json")

	#step_2b ~~
	#Isuues:
	#-Not sure what the best way of deduplicating something like "Furniture manufacturer" and "Furniture, Furnishings and Interiors Manufacturing: Wood Machining" is.
	#-"Youth Worker" in the standards dataset is "In development", while "Youth Work" level 2 and 3 are both available in the other dataset.
	#-"Baker" exists in both datasets, but there is a mismatch in duration (12 and 18 months)
	#-Many apprenticeships exist with multiple levels and/or length.

	unmatched_apprenticeships = dataset_match.loc[dataset_match['max_funding'].isnull() & dataset_match['level-detail'].notna()]
	unmatched_standards = dataset_match.loc[dataset_match['max_funding'].notna() & dataset_match['level-detail'].isnull()]

	#step_3
	print("Number of merged data points:",  len(standards) + len(apprenticeships) - len(dataset_match))
	print("Theoretical max merged data points without losing data:", min(len(standards), len(apprenticeships)))

	print("Unmatched apprenticeships:", len(unmatched_apprenticeships), "out of", len(apprenticeships))
	print("Unmatched apprenticeship standards:", len(unmatched_standards), "out of", len(standards))

	unmatched_apprenticeships_no_duplicates = unmatched_apprenticeships.drop_duplicates(subset = ['title'])
	print(len(unmatched_apprenticeships) - len(unmatched_apprenticeships_no_duplicates), "apprenticeships are duplicates (same title but different level and/or duration)")


if __name__ == '__main__':
	main()

