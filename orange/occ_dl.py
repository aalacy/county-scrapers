import csv
import os
import requests
from tqdm import tqdm
history_file = 'occompt_history.csv'
downloaded = 'downloaded.txt'
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
}
if os.path.isfile(history_file):
    with open(history_file) as f:
        found = list(csv.DictReader(f, skipinitialspace=True))
else:
    found = []

if os.path.isfile(downloaded):
    with open(downloaded) as f:
        dl = f.read().split('\n')
        dl = set(i for i in dl if i.strip())
else:
    dl = set()


def get_new_session():
    i_url = 'https://or.occompt.com/recorder/eagleweb/downloads/20080009420?id=DOCC29540700.A0&parent=DOCC29540700&preview=false&noredirect=true'
    local_ses = requests.Session()
    local_ses.proxies = {'http': 'socks5h://localhost:9050', 'https': 'socks5h://localhost:9050'}
    local_ses.headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }
    _ = local_ses.get(i_url)
    _ = local_ses.get('https://or.occompt.com/recorder/web/login.jsp')
    _ = local_ses.get('https://or.occompt.com/recorder/web/loginPOST.jsp?submit=I+Acknowledge&guest=true')
    return local_ses


ses = get_new_session()
os.makedirs('pdf_files', exist_ok=True)
os.chdir('pdf_files')
ind = 0
for item in tqdm(found):
    ind += 1
    if ind < 8219:
        dl.add(item['id'])
        continue
    if item['id'] in dl:
        continue
    try:
        r = ses.get(item['pdf_file'])
    except requests.exceptions.ConnectionError:
        break
    if r.status_code != 200:
        break
    fn = f"{item['id']}_{item['date'].split()[0].replace('/','-')}_{item['doc_type']}.pdf"
    with open(fn, 'wb') as f:
        f.write(r.content)
        dl.add(item['id'])

os.chdir('..')
with open(downloaded, 'w') as f:
    for i in dl:
        f.write(f"{i}\n")
print("Completed")
