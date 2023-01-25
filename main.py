from statics import *
import datetime
import os
import urllib.request
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from time import time as timer
from typing import List, Tuple
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from pprint import pprint
from upload import AmazonS3


def get_urls() -> list[tuple[int, str]]:
    results = []
    response = requests.get(BASE_URL_RECEITA)
    html_doc = response.content
    soup = BeautifulSoup(html_doc, 'html.parser')

    lista_link = soup.findAll('a', {'class', 'external-link'})
    urls = list(filter(lambda x: x, [link.get('href') for link in lista_link]))
    urls = [url for url in urls if
            any([tags.upper() in url for tags in ('SOCIOCSV', 'EMPRECSV', 'ESTABELE', 'SIMPLES')])]
    for url in urls:
        resp = urllib.request.urlopen(url)
        results.append(
            (int(resp.info().get('Content-Length')), url,)
        )
    results.sort(key=lambda x: x[0])
    return results


def async_download():
    print(f"INÍCIO :-> {datetime.datetime.now()}")
    start = timer()
    urls = get_urls()
    results = ThreadPool(cpu_count()).imap_unordered(download_unitario, urls[5:])
    for path in results:
        print(path)
    print(f"Tempo total(s) :-> {timer() - start}")


def download_unitario(url: tuple[int, str]):
    fname = url[1].split('/')[-1]
    # print(f"MB -> {int(resp.info().get('Content-Length')) / pow(1024, 2)}")
    # print(f"GB -> {int(resp.info().get('Content-Length')) / pow(1024, 3)}")

    with requests.get(url[1], stream=True) as resp:
        # total = int(resp.headers.get('content-length', 0))
        fname = os.path.join(BASE_PATH, 'downloads', fname)
        with open(fname, 'wb') as file, tqdm(
                desc=fname, total=url[0], unit='iB', unit_scale=True, unit_divisor=1024
        ) as bar:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                size = file.write(chunk)
                bar.update(size)


def upload_all_files():
    s3 = s3 = AmazonS3("", "", 'us-east-1')
    for root, dirs, files in os.walk(os.path.join(BASE_PATH, r'unziped')):
        for file in files:
            with open(os.path.join(BASE_PATH, r'unziped', file), 'rb') as data:
                s3.push_data_to_s3_bucket('zeta-internal-data', data, 'K3241.K03200Y1.D20312.EMPRECSV.zip',
                                          file_size=os.path.getsize(
                                              os.path.join(BASE_PATH, r'downloads', file)
                                          ), content_type='zip')


if __name__ == '__main__':
    # get_urls()

    # s3 = AmazonS3("", "", 'us-east-1')
    # with open(os.path.join(os.getcwd(), r'downloads/K3241.K03200Y1.D20312.EMPRECSV.zip'), 'rb') as data:
    #     s3.push_data_to_s3_bucket('zeta-internal-data', data, 'K3241.K03200Y1.D20312.EMPRECSV.zip',
    #                               file_size=os.path.getsize(
    #                                   os.path.join(BASE_PATH, r'downloads', r'K3241.K03200Y1.D20312.EMPRECSV.zip')),
    #                               content_type='zip')
    async_download()