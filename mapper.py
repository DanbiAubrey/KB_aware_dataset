import json
import re
import gzip

itm_dict = {} # key: iid , value: asin
itm_asin_lst = itm_dict.values() # list of asin of items
asin2title = {}

with open("mapper/amazon_item.json", 'r') as f:
    for line in f:
        item = json.loads(line.strip())  # 각 줄을 개별적으로 파싱
        #print(item)
        itm_dict[item['iid']] = item['asin']
i=0

path = 'origin_data/metadata.json.gz'

def parse(path):
    g = gzip.open(path, 'r')
    for l in g:
        yield eval(l)
    
for item in parse(path):
    if item['asin'] in itm_asin_lst and 'title' in item.keys():
        asin2title[item['asin']] = item['title']
        i += 1
        if i % 100 == 0:
            print(i)

file_path = "amazon_books_asin2title.json"

# Writing dictionary data to a CSV file
with open(file_path, mode='w', newline='') as json_file:
    writer = json.dump(asin2title, json_file, indent=4)
   
print(f"Data successfully saved to {file_path}")

print(asin2title)
print(len(asin2title))


