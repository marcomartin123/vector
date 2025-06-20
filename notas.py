import os
import pdfplumber
import re
import logging

# --- CONFIGURAÇÕES ---
# CAMINHO_DIRETORIO = "notas_de_corretagem" # Removed
# ARQUIVO_SAIDA_TXT = "notas_extraidas.txt" # Removed

CAMPOS_TAXAS = [
    "Taxa de liquidação",
    "Taxa de Registro",
    "Total Bovespa / Soma",
    "Total corretagem / Despesas"
]

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

CONFIGURATIONS = [
    {
        'person_type': 'M',
        'CAMINHO_DIRETORIO': os.path.join('notasm', 'notas_de_corretagem'),
        'ARQUIVO_SAIDA_TXT': 'notas_extraidas_m.txt'
    },
    {
        'person_type': 'R',
        'CAMINHO_DIRETORIO': os.path.join('notasr', 'notas_de_corretagem'),
        'ARQUIVO_SAIDA_TXT': 'notas_extraidas_r.txt'
    }
]

def limpar_numero(texto_numero):
    """
    Converte um número em formato string brasileiro (ex: '1.234,56') para um float (1234.56).
    """
    if not isinstance(texto_numero, str): return 0.0
    try:
        return float(texto_numero.replace('.', '').replace(',', '.'))
    except (ValueError, AttributeError):
        return 0.0

def parse_bloco_negocios(bloco_texto):
    """
    Analisa o bloco de texto de negociações e retorna uma lista de dicionários,
    com os dados de cada transação de forma estruturada para cálculos.
    """
    transacoes_encontradas = []
    padroes = [
        re.compile(r"VISTA\s+(.+?)\s+([\d\.]+)\s+([\d,]+)\s+([\d\.,]+)\s+([CD])$"),
        re.compile(r"\d{2}\/\d{2}\s+(\S+).+?([\d\.]+)\s+([\d,]+)\s+([\d\.,]+)\s+([CD])$")
    ]
    for linha in bloco_texto.split('\n'):
        linha_strip = linha.strip()
        if not linha_strip or linha_strip.startswith("Q Negociação"): continue
        for padrao in padroes:
            match = padrao.search(linha_strip)
            if match:
                try:
                    transacao = {
                        'tipo': match.group(5),
                        'ativo': match.group(1).strip().replace("  ", " "),
                        'quantidade_str': match.group(2),
                        'preco_str': match.group(3),
                        'valor_op_str': match.group(4),
                        'valor_op_num': limpar_numero(match.group(4))
                    }
                    transacoes_encontradas.append(transacao)
                    break
                except IndexError:
                    logging.warning(f"Regex encontrou uma linha, mas falhou ao extrair grupos: '{linha_strip}'")
    return transacoes_encontradas


def processar_arquivos_pdf():
    """
    Função principal que orquestra a leitura, processamento, cálculo e escrita.
    Itera sobre as configurações definidas em CONFIGURATIONS.
    """
    for config in CONFIGURATIONS:
        CAMINHO_DIRETORIO = config['CAMINHO_DIRETORIO']
        ARQUIVO_SAIDA_TXT = config['ARQUIVO_SAIDA_TXT']
        logging.info(f"--- Processando para {config['person_type']} ---")
        if not os.path.isdir(CAMINHO_DIRETORIO):
            logging.error(f"O diretório '{CAMINHO_DIRETORIO}' para {config['person_type']} não foi encontrado.")
            continue # Pula para a próxima configuração

        notas_agrupadas = {}
        arquivos_pdf = sorted([f for f in os.listdir(CAMINHO_DIRETORIO) if f.lower().endswith('.pdf')])
        logging.info(f"Encontrados {len(arquivos_pdf)} arquivos PDF para processar em {CAMINHO_DIRETORIO}.")

        for nome_arquivo in arquivos_pdf:
            caminho_completo = os.path.join(CAMINHO_DIRETORIO, nome_arquivo)
            try:
                with pdfplumber.open(caminho_completo) as pdf:
                    texto_completo = "\n".join([p.extract_text() for p in pdf.pages if p.extract_text()])
                # Extrai o número da nota da primeira página para agrupar corretamente
                primeira_linha_dados = texto_completo.split('\n')[2]
                numero_nota = primeira_linha_dados.split()[0].strip()

                if numero_nota not in notas_agrupadas:
                    notas_agrupadas[numero_nota] = {'texto_completo': '', 'arquivos': []}

                notas_agrupadas[numero_nota]['texto_completo'] += texto_completo + "\n"
                notas_agrupadas[numero_nota]['arquivos'].append(nome_arquivo)
            except Exception as e:
                logging.error(f"Ocorreu um erro fatal ao ler '{nome_arquivo}': {e}")

        resultados_finais_formatados = []
        for numero_nota, dados_nota in notas_agrupadas.items():
            logging.info(f"=== Processando e Calculando Nota: {numero_nota} para {config['person_type']} ===")
            texto_consolidado = dados_nota['texto_completo']
            nomes_arquivos = ", ".join(dados_nota['arquivos'])

            # --- NOVA LÓGICA: EXTRAIR DATA DO PREGÃO ---
            data_pregao = "N/D"
            try:
                # A linha 3 (índice 2) contém: Nr.Nota Folha Data
                partes_linha_dados = texto_consolidado.split('\n')[2].split()
                if len(partes_linha_dados) >= 3:
                    data_pregao = partes_linha_dados[2]
            except IndexError:
                logging.warning(f"Não foi possível extrair a data do pregão para a nota {numero_nota}.")

            # --- Cálculo das despesas (permanece igual) ---
            total_despesas_nota = 0.0
            taxas_para_exibir = {}
            for campo in CAMPOS_TAXAS:
                match = re.search(re.escape(campo) + r'\s+(\S+)', texto_consolidado)
                valor_str = match.group(1) if match else "0,00"
                taxas_para_exibir[campo] = valor_str
                total_despesas_nota += limpar_numero(valor_str)
            logging.info(f"Despesas totais da nota {numero_nota}: {total_despesas_nota:.2f}")

            # --- Extração das transações (permanece igual) ---
            todos_blocos_de_negocios = ""
            partes = texto_consolidado.split("Negócios realizados")
            for i, parte in enumerate(partes):
                if i > 0 and "Resumo dos Negócios" in parte:
                    todos_blocos_de_negocios += parte.split("Resumo dos Negócios")[0] + "\n"

            transacoes = parse_bloco_negocios(todos_blocos_de_negocios)
            logging.info(f"Encontradas {len(transacoes)} transações na nota {numero_nota}.")

            # --- Cálculo do rateio (permanece igual) ---
            if transacoes:
                valor_total_operacoes_nota = sum(t['valor_op_num'] for t in transacoes)
                for t in transacoes:
                    despesa_proporcional = 0.0
                    if valor_total_operacoes_nota > 0:
                        proporcao = t['valor_op_num'] / valor_total_operacoes_nota
                        despesa_proporcional = total_despesas_nota * proporcao

                    if t['tipo'] == 'D':
                        valor_final_num = t['valor_op_num'] + despesa_proporcional
                    else:
                        valor_final_num = t['valor_op_num'] - despesa_proporcional

                    t['valor_final_calculado'] = valor_final_num

            # --- MONTAGEM DA SAÍDA FINAL (COM A NOVA COLUNA DE DATA) ---
            resultados_finais_formatados.append(f"--- Nota: {numero_nota} (Arquivos: {nomes_arquivos}) ---")
            for campo, valor in taxas_para_exibir.items():
                resultados_finais_formatados.append(f"{campo}: {valor}")

            for t in transacoes:
                valor_final_calculado = t.get('valor_final_calculado', t['valor_op_num'])
                valor_final_str = f"{valor_final_calculado:_.2f}".replace('.',',').replace('_','.')

                # Formato da linha atualizado para incluir a data do pregão
                linha_formatada = (f"{t['tipo']}|{t['ativo']}|{data_pregao}|{t['quantidade_str']}|"
                                   f"{t['preco_str']}|{t['valor_op_str']}|{valor_final_str}")
                resultados_finais_formatados.append(linha_formatada)

            resultados_finais_formatados.append("")

        if resultados_finais_formatados: # Certifique-se de que esta verificação está dentro do loop de config
            with open(ARQUIVO_SAIDA_TXT, 'w', encoding='utf-8') as f:
                f.write('\n'.join(resultados_finais_formatados))
            logging.info(f"\nProcessamento para {config['person_type']} concluído! Resultados salvos em '{ARQUIVO_SAIDA_TXT}'")
        else: # Adicionado para clareza do log
            logging.info(f"Nenhum resultado final formatado para {config['person_type']} em {CAMINHO_DIRETORIO}.")

if __name__ == "__main__":
    processar_arquivos_pdf()