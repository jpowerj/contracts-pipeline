import os, json
from collections import defaultdict

path = '/cluster/work/lawecon/Work/dominik/powerparser/output_all_data/01_artsplit'

d = defaultdict(list)
files = os.listdir(path)
headers = []
for i,fn in enumerate(files):
	if not "eng" in fn:
		continue
	if i % 1000 == 0:
		print (i)
	with open(os.path.join(path, fn)) as f:
		info = json.loads(f.read())
		for item in info["articles"]:
			#print (item["header"])
			try:
				header = item["header"]["text"]
				headers.append(header)
				d[fn].append(header)
			except:
				pass
print (headers[:10])
print (len(headers))
print (len(set(headers)))

with open("all_headers.txt", "w") as f:
	for key, val in d.items():
		f.write(key + "\t" + "\t".join(val) + "\n")
