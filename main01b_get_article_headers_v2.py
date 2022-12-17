import os, json
from collections import defaultdict

path = '/cluster/work/lawecon/Work/dominik/powerparser/output_all_data/01_artsplit'

d = defaultdict(list)
files = os.listdir(path)
headers = []

#        article_strs = [(article_as_str(cur_art), {'contract_id': contract_id, 'article_num':art_num}) 
#                        for art_num, cur_art in enumerate(art_list)]


with open("all_headers_v2.txt", "w") as outfile:
	for i,fn in enumerate(files):
		if not "eng" in fn:
			continue
		if i % 1000 == 0:
			print (i)
		with open(os.path.join(path, fn)) as f:
			contract_data = json.loads(f.read())

			contract_id = contract_data["contract_id"]
			art_list = contract_data["articles"]

			for art_num, cur_art in enumerate(art_list):
				#print (item["header"])
				try:
					header = cur_art["header"]["text"]
					#print (contract_id, art_num, header)
					#input("")
					outfile.write(contract_id + "\t" + str(art_num) + "\t" + header + "\n")
				except Exception as e:
					print (str(e))
					pass

