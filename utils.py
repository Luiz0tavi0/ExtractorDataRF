import os
import multiprocessing as mp
from pool_unzip_files import unzip_unitario
from statics import BASE_PATH


# gerar callback pra função principal baixar e processar novamente quaisquer arquivos corrompidos


def fanout_unzip(path):
    """create pool to extract all"""
    files_to_unzip = []
    files_to_re_download = []
    for root, dirs, files in os.walk(path):
        for i in files:
            if i.endswith(".zip"):
                files_to_unzip.append((root, i,))

    with mp.Pool(min(mp.cpu_count(), len(files_to_unzip))) as pool:  # number of workers
        fail = pool.starmap(unzip_unitario, files_to_unzip, chunksize=1)
        files_to_re_download.extend(fail)
    files_to_re_download = list(set([file for file in files_to_re_download if file is not None]))
    return files_to_re_download


if __name__ == "__main__":
    path = os.path.join(BASE_PATH, 'downloads')
    refazer = fanout_unzip(path)
    print(refazer)
    print('Extração Completa')
