import os
from pathlib import Path
BASE_PATH = os.getcwd()
BASE_URL_RECEITA = r"https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj"
CHUNK_SIZE = 8000

Path(os.path.join(BASE_PATH, 'downloads')).mkdir(exist_ok=True)
Path(os.path.join(BASE_PATH, 'unziped')).mkdir(exist_ok=True)
