import requests
import os
import time
from pathlib import Path

def download_b3_series_robusta(url, max_retries=3, save_dir="."):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://www.b3.com.br/',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }
    
    # Criar diretório
    Path(save_dir).mkdir(exist_ok=True)
    
    for attempt in range(max_retries):
        try:
            print(f"Tentativa {attempt + 1} de {max_retries}")
            
            response = requests.get(url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()
            
            # Verificar se realmente recebeu um arquivo
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' in content_type:
                print("Recebeu HTML em vez do arquivo. Tentando novamente...")
                time.sleep(2)
                continue
            
            # Nome do arquivo
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"series_autorizadas_b3_{timestamp}.txt"
            
            content_disposition = response.headers.get('Content-Disposition')
            if content_disposition and 'filename=' in content_disposition:
                filename = content_disposition.split('filename=')[1].strip('"\'')
            
            filepath = Path(save_dir) / filename
            
            # Baixar
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Verificar se o arquivo não está vazio
            if filepath.stat().st_size == 0:
                print("Arquivo baixado está vazio. Tentando novamente...")
                filepath.unlink()  # Deletar arquivo vazio
                time.sleep(2)
                continue
            
            print(f"Download bem-sucedido: {filepath}")
            print(f"Tamanho: {filepath.stat().st_size} bytes")
            return str(filepath)
            
        except requests.exceptions.RequestException as e:
            print(f"Erro na tentativa {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)  # Aguardar antes de tentar novamente
            else:
                print("Todas as tentativas falharam")
                return None

# Executar
url = "https://www.b3.com.br/lumis/portal/file/fileDownload.jsp?fileId=8AE490CA9781882C01978D56DCAE286D"
arquivo_baixado = download_b3_series_robusta(url)

if arquivo_baixado:
    print(f"Arquivo salvo em: {arquivo_baixado}")
else:
    print("Falha no download")