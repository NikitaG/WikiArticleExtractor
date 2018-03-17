import subprocess

bzip = subprocess.Popen('pigz -d -c /Users/nikitagolovkin/Projects/wikiConverter/data/latest-all.json.gz'.split(), stdout=subprocess.PIPE)
#grep = subprocess.Popen('grep ntp'.split(), stdin=ls.stdout, stdout=subprocess.PIPE)
#ls.stdout.close()
#output = grep.communicate()[0]
#ls.wait()
print(bzip.stdout.readline())
print(bzip.stdout.readline())
#print(ls.communicate()[0])