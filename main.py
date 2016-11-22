import urllib.request
import re
from bs4 import BeautifulSoup
import pdb
import os
import multiprocessing
from multiprocessing import Pool, Value
import urllib.request
global total_entries

#Function to be run by each thread. Makes the http request to j-archive and writes the result to the csv file
def makeRequest(contestant_number):
	
	with urllib.request.urlopen('http://www.j-archive.com/showplayerstats.php?player_id=' + str(contestant_number)) as response:
		html = response.read()
		soup = BeautifulSoup(html, 'html.parser')

		#Searches the HTML for the tag with class player_occupation_and_origin, which has some data we want
		occString = soup.find("p",class_="player_occupation_and_origin")
		if occString is None: #If it can't find that tag, it skips it
			print ("Contestant number " + str(contestant_number) + " skipped, page not loaded correctly")
			return
		occString = occString.text
		#The occString is in the format "A(n?) ___(1)___ (originally)? from __(2)__, __(3)__". (1),(2),and (3) are the 
		#occupation, city, and state
		splits = list(map(  (lambda c:c.strip()) ,filter(None,re.split("An?(.*)(originally)?from(.*)\.\.\.",occString))))

		#If searching the occString produces something not in that format, we skip it
		if len(splits) != 2:
			print ("Contestant number " + str(contestant_number) + " skipped due to improper formatting")
			return
		
		#Parses the table to get the money totals
		dollar_sum=0
		for elem in soup(text=re.compile('#\d+,')):
			
			raw_text = elem.parent.parent.next_sibling.next_sibling.next_sibling.text #Why can't this be like jQuery?
			raw_text = re.sub('\D','',raw_text)#Strips non number characters
			dollar_sum += int(raw_text)

		#Cleans the text
		occupation = re.sub('originally','',splits[0]).strip() 
		locationSplits = splits[1].split(', ');
		if len(locationSplits) < 2:
			state = locationSplits[0]
			city = ""
		else:
			city = locationSplits[0]
			state = locationSplits[1]
		#print (occupation)
		#print (city)
		#print (state)
		print("PID: " + str(os.getpid()) + " processing " + str(contestant_number))

		#Acquires a lock, then attempts to write the dollar sum and occupation/origin data to the csv
		lock.acquire()
		try:
			f.write(str(dollar_sum) + ',' + occupation + "," + state + "," + city + '\n')
			#total_entries.value += 1
		finally:
			lock.release()
	return

maxContestantNumber=10199

f = open('results.csv','w')
lock = multiprocessing.Lock()

if __name__ == '__main__':
	with Pool(processes=multiprocessing.cpu_count()) as thread_pool:
		#global total_entries
		#total_entries = Value("i",0)
		thread_pool.map(makeRequest,range(maxContestantNumber))
		#print ("total_entries: " + str(total_entries.value))



