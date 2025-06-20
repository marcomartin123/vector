# down.py
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime
import os
import sys
import time

def download_series_autorizadas():
    """
    Realiza o download do arquivo "Séries Autorizadas" do site da B3 usando Playwright.

    Esta abordagem controla um navegador real para contornar proteções complexas
    (como Cloudflare) que podem bloquear requisições diretas.

    O processo é:
    1. Navegar até a página de Séries Autorizadas.
    2. Encontrar o link de texto "Lista Completa de Séries Autorizadas".
    3. Clicar no link e capturar o download resultante.

    Retorna:
        bool: True se o download for bem-sucedido, False caso contrário.
    """
    print("--- Iniciando download do arquivo de Séries Autorizadas da B3 (usando Playwright) ---")

    FILENAME = "SI_D_SEDE.zip"
    URL_PAGE = "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/mercado-a-vista/opcoes/series-autorizadas/"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            )
            page = context.new_page()

            print(f"1. Navegando para a página: {URL_PAGE}")
            page.goto(URL_PAGE, timeout=9000000)

            # --- MUDANÇA PRINCIPAL AQUI ---
            # Em vez de procurar por um seletor de href, procuramos pelo texto exato do link.
            # Esta é a forma mais confiável de encontrar o elemento.
            download_link_selector = page.get_by_text("Lista Completa de Séries Autorizadas", exact=True)
            
            print("2. Aguardando o link de download ficar disponível...")
            download_link_selector.wait_for(state="visible", timeout=6000000)
            
            print("3. Iniciando a captura do download e clicando no link...")
            
            with page.expect_download(timeout=6000000) as download_info:
                download_link_selector.click()
            
            download = download_info.value
            
            if os.path.exists(FILENAME):
                os.remove(FILENAME)
                print(f"   Arquivo antigo '{FILENAME}' removido.")

            download.save_as(FILENAME)
            
            print(f"\n[SUCESSO] Download concluído! Arquivo salvo como '{FILENAME}'")
            
            browser.close()
            return True

    except PlaywrightTimeoutError:
        print("\n[ERRO] Timeout: A página ou o link de download demorou demais para carregar.")
        print("   Isso pode ser devido a uma conexão lenta ou a uma mudança no site da B3.")
        return False
    except Exception as e:
        print(f"\n[ERRO] Ocorreu um erro inesperado com o Playwright: {e}")
        return False

if __name__ == "__main__":
    print("Executando o script de download de forma autônoma para teste...")
    success = download_series_autorizadas()
    if success:
        print("\nTeste finalizado com sucesso.")
    else:
        print("\nTeste finalizado com erros.")