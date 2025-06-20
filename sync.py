#--- START OF FILE sync.py ---

import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime, timedelta # Adicionado timedelta
import os # Para construir caminhos de arquivo de forma segura

def conectar_mt5():
    """Conecta ao MetaTrader 5"""
    if not mt5.initialize():
        print(f"Erro ao inicializar MT5: {mt5.last_error()}")
        return False
    
    print(f"MT5 inicializado com sucesso!")
    print(f"Versão: {mt5.version()}")
    print(f"Informações da conta: {mt5.account_info()}")
    return True

def obter_todos_simbolos():
    """Obtém todos os símbolos disponíveis no MT5"""
    simbolos = mt5.symbols_get()
    
    if simbolos is None:
        print("Erro ao obter símbolos")
        return None
    
    print(f"Total de símbolos encontrados: {len(simbolos)}")
    return simbolos

def processar_simbolos(simbolos):
    """Processa os símbolos e converte para lista de dicionários"""
    dados_simbolos = []
    
    for simbolo in simbolos:
        info_simbolo = mt5.symbol_info(simbolo.name)
        
        if info_simbolo is None:
            continue
            
        dados = {
            'nome': info_simbolo.name,
            'descricao': info_simbolo.description,
            'categoria': info_simbolo.category,
            'moeda_base': info_simbolo.currency_base,
            'moeda_lucro': info_simbolo.currency_profit,
            'moeda_margem': info_simbolo.currency_margin,
            'banco': info_simbolo.bank,
            'bolsa': info_simbolo.exchange,
            'formula': info_simbolo.formula,
            'isin': info_simbolo.isin,
            'pagina': info_simbolo.page,
            'caminho': info_simbolo.path,
            'base': info_simbolo.basis,
            'personalizado': info_simbolo.custom,
            'modo_grafico': info_simbolo.chart_mode,
            'selecionado': info_simbolo.select,
            'visivel': info_simbolo.visible,
            'digitos': info_simbolo.digits,
            'spread': info_simbolo.spread,
            'spread_flutuante': info_simbolo.spread_float,
            'profundidade_book': info_simbolo.ticks_bookdepth,
            'modo_calculo_negociacao': info_simbolo.trade_calc_mode,
            'modo_negociacao': info_simbolo.trade_mode,
            'tempo_inicio': datetime.fromtimestamp(info_simbolo.start_time) if info_simbolo.start_time > 0 else None,
            'tempo_expiracao': datetime.fromtimestamp(info_simbolo.expiration_time) if info_simbolo.expiration_time > 0 else None,
            'nivel_stops': info_simbolo.trade_stops_level,
            'nivel_freeze': info_simbolo.trade_freeze_level,
            'modo_execucao': info_simbolo.trade_exemode,
            'modo_swap': info_simbolo.swap_mode,
            'rollover_3_dias': info_simbolo.swap_rollover3days,
            'margem_hedge_usa_perna': info_simbolo.margin_hedged_use_leg,
            'swap_long': info_simbolo.swap_long,
            'swap_short': info_simbolo.swap_short,
            'modo_expiracao': info_simbolo.expiration_mode,
            'modo_preenchimento': info_simbolo.filling_mode,
            'modo_ordem': info_simbolo.order_mode,
            'modo_gtc_ordem': info_simbolo.order_gtc_mode,
            'modo_opcao': info_simbolo.option_mode,
            'direito_opcao': info_simbolo.option_right,
            'strike_opcao': info_simbolo.option_strike, # Este strike ainda será o do MT5 aqui
            'bid': info_simbolo.bid,
            'bid_alto': info_simbolo.bidhigh,
            'bid_baixo': info_simbolo.bidlow,
            'ask': info_simbolo.ask,
            'ask_alto': info_simbolo.askhigh,
            'ask_baixo': info_simbolo.asklow,
            'ultimo': info_simbolo.last,
            'ultimo_alto': info_simbolo.lasthigh,
            'ultimo_baixo': info_simbolo.lastlow,
            'volume': info_simbolo.volume,
            'volume_alto': info_simbolo.volumehigh,
            'volume_baixo': info_simbolo.volumelow,
            'volume_real': info_simbolo.volume_real,
            'volume_alto_real': info_simbolo.volumehigh_real,
            'volume_baixo_real': info_simbolo.volumelow_real,
            'ponto': info_simbolo.point,
            'valor_tick_negociacao': info_simbolo.trade_tick_value,
            'valor_tick_lucro': info_simbolo.trade_tick_value_profit,
            'valor_tick_perda': info_simbolo.trade_tick_value_loss,
            'tamanho_tick': info_simbolo.trade_tick_size,
            'tamanho_contrato': info_simbolo.trade_contract_size,
            'juros_acumulados': info_simbolo.trade_accrued_interest,
            'valor_nominal': info_simbolo.trade_face_value,
            'taxa_liquidez': info_simbolo.trade_liquidity_rate,
            'volume_min': info_simbolo.volume_min,
            'volume_max': info_simbolo.volume_max,
            'volume_step': info_simbolo.volume_step,
            'volume_limite': info_simbolo.volume_limit,
            'margem_inicial': info_simbolo.margin_initial,
            'margem_manutencao': info_simbolo.margin_maintenance,
            'margem_hedge': info_simbolo.margin_hedged,
            'time': datetime.fromtimestamp(info_simbolo.time) if info_simbolo.time > 0 else None,
            'negociacoes_sessao': info_simbolo.session_deals,
            'ordens_compra_sessao': info_simbolo.session_buy_orders,
            'ordens_venda_sessao': info_simbolo.session_sell_orders,
            'volume_sessao': info_simbolo.session_volume,
            'giro_sessao': info_simbolo.session_turnover,
            'interesse_sessao': info_simbolo.session_interest,
            'volume_ordens_compra_sessao': info_simbolo.session_buy_orders_volume,
            'volume_ordens_venda_sessao': info_simbolo.session_sell_orders_volume,
            'abertura_sessao': info_simbolo.session_open,
            'fechamento_sessao': info_simbolo.session_close,
            'preco_medio_ponderado_sessao': info_simbolo.session_aw,
            'preco_liquidacao_sessao': info_simbolo.session_price_settlement,
            'limite_preco_min_sessao': info_simbolo.session_price_limit_min,
            'limite_preco_max_sessao': info_simbolo.session_price_limit_max,
            'mudanca_preco': info_simbolo.price_change,
            'volatilidade_preco': info_simbolo.price_volatility,
            'preco_teorico': info_simbolo.price_theoretical,
            'delta': info_simbolo.price_greeks_delta,
            'theta': info_simbolo.price_greeks_theta,
            'gamma': info_simbolo.price_greeks_gamma,
            'vega': info_simbolo.price_greeks_vega,
            'rho': info_simbolo.price_greeks_rho,
            'omega': info_simbolo.price_greeks_omega,
            'sensibilidade_preco': info_simbolo.price_sensitivity
        }
        dados_simbolos.append(dados)
    return dados_simbolos

def extrair_ativo_do_isin(isin):
    if not isin or len(isin) < 7:
        return None
    return isin[2:7]

def identificar_tipo_opcao(ticker):
    if not ticker or len(ticker) < 5:
        return None
    quinta_letra = ticker[4].upper()
    if quinta_letra in 'ABCDEFGHIJKL':
        return 'CALL'
    elif quinta_letra in 'MNOPQRSTUVWX':
        return 'PUT'
    else:
        return None

def extrair_dados_opcao(ticker, isin, strike_mt5, tempo_expiracao_dt):
    """Extrai dados da opção: ativo, strike (do MT5 inicialmente), expiração, tipo"""
    ativo = extrair_ativo_do_isin(isin)
    tipo = identificar_tipo_opcao(ticker)
    
    if not ativo or not tipo:
        return None
    
    expiracao_formatada = None
    if isinstance(tempo_expiracao_dt, datetime):
        expiracao_formatada = tempo_expiracao_dt.strftime('%d/%m/%Y')
    
    return {
        'ativo': ativo,
        'tipo': tipo,
        'strike': strike_mt5, # Este é o strike do MT5, usado para o merge inicial
        'expiracao': expiracao_formatada,
        'ticker': ticker
    }

def agrupar_opcoes_call_put(dados):
    """Agrupa opções call/put e filtra por data de expiração."""
    opcoes = []
    for item in dados:
        if item.get('isin') and item.get('nome'):
            dados_opcao = extrair_dados_opcao(
                item['nome'], 
                item['isin'], 
                item.get('strike_opcao', 0), # Strike do MT5
                item.get('tempo_expiracao')
            )
            if dados_opcao:
                opcoes.append(dados_opcao)
    
    df_opcoes = pd.DataFrame(opcoes)
    if df_opcoes.empty:
        print("Nenhuma opção encontrada para processamento em DataFrame")
        return pd.DataFrame()
    
    calls = df_opcoes[df_opcoes['tipo'] == 'CALL'].copy()
    puts = df_opcoes[df_opcoes['tipo'] == 'PUT'].copy()

    if calls.empty or puts.empty:
        print("Não foram encontradas calls ou puts suficientes para formar pares.")
        if calls.empty: print(f"Total de calls processadas: 0")
        else: print(f"Total de calls processadas: {len(calls)}")
        if puts.empty: print(f"Total de puts processadas: 0")
        else: print(f"Total de puts processadas: {len(puts)}")
        return pd.DataFrame()
            
    pares_opcoes = calls.merge(
        puts, 
        on=['ativo', 'strike', 'expiracao'], # Merge usa o strike do MT5
        how='inner',
        suffixes=('_call', '_put')
    )
    
    if pares_opcoes.empty:
        print("Nenhum par call/put encontrado após o merge inicial.")
        return pd.DataFrame()

    # --- INÍCIO DA LÓGICA DE FILTRO DE DATA ---
    hoje = datetime.now()
    data_limite = hoje + timedelta(days=10) 
    print(f"Filtrando opções com expiração anterior a {data_limite.strftime('%d/%m/%Y')}")
    pares_opcoes['expiracao_dt'] = pd.to_datetime(pares_opcoes['expiracao'], format='%d/%m/%Y', errors='coerce')
    pares_opcoes_filtrado = pares_opcoes.dropna(subset=['expiracao_dt'])
    pares_opcoes_filtrado = pares_opcoes_filtrado[pares_opcoes_filtrado['expiracao_dt'].dt.date >= data_limite.date()]
    
    print(f"Pares antes do filtro de data: {len(pares_opcoes)}. Pares após filtro de data: {len(pares_opcoes_filtrado)}")
    
    if pares_opcoes_filtrado.empty:
        print(f"Nenhum par call/put encontrado após o filtro de data (expiração >= {data_limite.strftime('%d/%m/%Y')}).")
        return pd.DataFrame()
    # --- FIM DA LÓGICA DE FILTRO DE DATA ---

    resultado = pares_opcoes_filtrado[['ativo', 'ticker_call', 'ticker_put', 'strike', 'expiracao']].copy()
    resultado.columns = ['ativo_principal', 'ticker_call', 'ticker_put', 'strike_mt5', 'expiracao'] 
    resultado = resultado.sort_values(['ativo_principal', 'expiracao', 'strike_mt5'])
    
    return resultado

def carregar_strikes_externos(caminho_arquivo_strikes):
    strikes_map = {}
    linhas_relevantes = []
    try:
        print(f"Iniciando leitura do arquivo de strikes: {caminho_arquivo_strikes}")
        encodings_to_try = ['latin1', 'utf-8']
        file_content_read = False
        for encoding in encodings_to_try:
            try:
                with open(caminho_arquivo_strikes, 'r', encoding=encoding) as f:
                    for linha_num, linha_raw in enumerate(f):
                        if linha_raw.startswith("02|"):
                            campos = linha_raw.strip().split('|')
                            linhas_relevantes.append(campos)
                file_content_read = True
                print(f"Arquivo lido com encoding: {encoding}. Total de linhas começando com '02': {len(linhas_relevantes)}")
                break 
            except UnicodeDecodeError:
                print(f"Falha ao ler com encoding {encoding}, tentando próximo...")
                linhas_relevantes = [] 
            except FileNotFoundError:
                print(f"❌ ERRO: Arquivo de strikes não encontrado em {caminho_arquivo_strikes}")
                return strikes_map
            except Exception as e_open:
                print(f"❌ ERRO ao abrir ou ler arquivo com {encoding}: {e_open}")
                linhas_relevantes = [] 
        if not file_content_read:
            print("❌ ERRO: Não foi possível ler o arquivo de strikes com os encodings testados.")
            return strikes_map
        if not linhas_relevantes:
            print("⚠️ Nenhuma linha começando com '02' encontrada no arquivo de strikes.")
            return strikes_map
        df_full = pd.DataFrame(linhas_relevantes)
        if df_full.empty:
            print("⚠️ DataFrame vazio após filtrar linhas '02'.")
            return strikes_map
        idx_ticker = 13
        idx_strike = 16
        if max(idx_ticker, idx_strike) >= len(df_full.columns):
            print(f"❌ ERRO: As linhas '02' não possuem colunas suficientes. "
                  f"Necessário até índice {max(idx_ticker, idx_strike)}, mas as linhas '02' têm {len(df_full.columns)} colunas (índices 0 a {len(df_full.columns)-1}).")
            print(f"Número de colunas detectado para linhas '02': {len(df_full.columns)}")
            if not df_full.empty: print(f"Primeiras 5 linhas '02' processadas (para depuração):\n{df_full.head().to_string()}")
            if idx_ticker >= len(df_full.columns): print(f"Coluna Ticker (índice {idx_ticker}) estaria fora dos limites.")
            if idx_strike >= len(df_full.columns): print(f"Coluna Strike (índice {idx_strike}) estaria fora dos limites.")
            return strikes_map
        df_strikes = pd.DataFrame({
            'ticker': df_full.iloc[:, idx_ticker].astype(str),
            'strike_externo_str': df_full.iloc[:, idx_strike].astype(str)
        })
        df_strikes['ticker'] = df_strikes['ticker'].str.strip()
        df_strikes['strike_externo_float'] = pd.to_numeric(
            df_strikes['strike_externo_str'].str.replace(',', '.', regex=False),
            errors='coerce'
        )
        df_strikes.dropna(subset=['ticker', 'strike_externo_float'], inplace=True)
        df_strikes = df_strikes[df_strikes['ticker'].str.strip() != '']
        if df_strikes.empty:
            print("⚠️ Nenhum strike válido encontrado nas linhas '02' após processamento e filtragem.")
            if not df_full.empty and idx_ticker < len(df_full.columns) and idx_strike < len(df_full.columns):
                temp_debug_df = pd.DataFrame({
                    f'ticker_bruto_col{idx_ticker}': df_full.iloc[:10, idx_ticker], 
                    f'strike_bruto_col{idx_strike}': df_full.iloc[:10, idx_strike]
                })
                print("Dados brutos (primeiras 10 linhas '02') das colunas de ticker e strike:")
                print(temp_debug_df.to_string())
            return strikes_map
        strikes_map = df_strikes.set_index('ticker')['strike_externo_float'].to_dict()
        print(f"✅ Strikes externos carregados: {len(strikes_map)} tickers mapeados a partir das linhas '02'.")
        if len(strikes_map) > 0:
            first_key = next(iter(strikes_map), None)
            if first_key:
                print(f"Exemplo de strike carregado: ('{first_key}', {strikes_map[first_key]})")
    except Exception as e:
        print(f"❌ ERRO inesperado ao carregar ou processar o arquivo de strikes: {e}")
        import traceback
        traceback.print_exc()
    return strikes_map

def salvar_csv_opcoes(dados_mt5, nome_arquivo_saida, strikes_externos_map):
    try:
        print("Processando pares de opções call/put...")
        df_opcoes = agrupar_opcoes_call_put(dados_mt5) 
        if df_opcoes.empty:
            print("❌ Nenhum par call/put encontrado após agrupamento inicial e filtro de data.")
            return False
        print(f"Pares encontrados antes do ajuste de strike (após filtro de data): {len(df_opcoes)}")
        if not strikes_externos_map:
            print("⚠️ Mapa de strikes externos está vazio. Strikes não serão ajustados com dados externos.")
            df_opcoes_final = df_opcoes.rename(columns={'strike_mt5': 'strike'})
            if 'strike' not in df_opcoes_final.columns: 
                 print("❌ Coluna 'strike_mt5' não encontrada para renomear.")
                 return False
        else:
            df_opcoes['strike'] = df_opcoes['ticker_call'].map(strikes_externos_map)
            strikes_encontrados = df_opcoes['strike'].notna().sum()
            total_pares = len(df_opcoes)
            print(f"Strikes externos encontrados para {strikes_encontrados} de {total_pares} tickers de call.")
            if strikes_encontrados < total_pares and strikes_encontrados < 20: 
                nao_encontrados = df_opcoes[df_opcoes['strike'].isna()]['ticker_call'].unique()
                print(f"Exemplo de tickers de CALL não encontrados no mapa de strikes: {list(nao_encontrados[:10])}")
            if 'strike' in df_opcoes.columns: 
                df_opcoes['strike'] = df_opcoes['strike'].fillna(df_opcoes['strike_mt5'])
            df_opcoes_final = df_opcoes.copy() 
            df_opcoes_final.dropna(subset=['strike'], inplace=True)
        if 'strike' not in df_opcoes_final.columns or df_opcoes_final.empty:
            print("❌ Nenhum par call/put com strike válido após processamento (ou mapa de strikes vazio e strike_mt5 ausente).")
            return False

        # --- INÍCIO DA NOVA REGRA DE FILTRO ---
        print(f"Pares antes do filtro de tickers terminados em 'E': {len(df_opcoes_final)}")
        if 'ticker_call' in df_opcoes_final.columns and 'ticker_put' in df_opcoes_final.columns:
            # Strip whitespace from ticker columns IN PLACE before checking.
            # This ensures the filter works correctly even if tickers have leading/trailing spaces.
            # Using .loc to avoid SettingWithCopyWarning and modify the DataFrame directly.
            df_opcoes_final.loc[:, 'ticker_call'] = df_opcoes_final['ticker_call'].str.strip()
            df_opcoes_final.loc[:, 'ticker_put'] = df_opcoes_final['ticker_put'].str.strip()

            # Now, proceed with the filter logic on the stripped tickers
            condition_call_ends_E = df_opcoes_final['ticker_call'].str.upper().str.endswith('E', na=False)
            condition_put_ends_E = df_opcoes_final['ticker_put'].str.upper().str.endswith('E', na=False)
            
            df_opcoes_final = df_opcoes_final[~(condition_call_ends_E | condition_put_ends_E)]
            print(f"Pares após o filtro de tickers terminados em 'E': {len(df_opcoes_final)}")

            if df_opcoes_final.empty:
                print("❌ Nenhum par call/put restante após o filtro de tickers terminados em 'E'.")
                return False
        else:
            print("⚠️ Colunas 'ticker_call' ou 'ticker_put' não encontradas. Filtro de tickers terminados em 'E' não aplicado.")
        # --- FIM DA NOVA REGRA DE FILTRO ---
            
        df_opcoes_final = df_opcoes_final[['ativo_principal', 'ticker_call', 'ticker_put', 'strike', 'expiracao']]
        df_opcoes_final = df_opcoes_final.sort_values(['ativo_principal', 'expiracao', 'strike'])
        df_opcoes_final['strike'] = df_opcoes_final['strike'].apply(
            lambda x: f"{x:.2f}".replace('.', ',') if pd.notnull(x) and isinstance(x, (int, float)) else x
        )
        df_opcoes_final.to_csv(
            nome_arquivo_saida,
            sep=';',
            decimal=',',
            index=False,
            encoding='utf-8-sig'
        )
        print(f"✅ Arquivo de opções salvo em: {nome_arquivo_saida}")
        print(f"📊 Total de pares call/put salvos: {len(df_opcoes_final)}")
        if not df_opcoes_final.empty:
            print(f"\n📈 Estatísticas das Opções Salvas:")
            print(f"- Ativos principais únicos com opções: {df_opcoes_final['ativo_principal'].nunique()}")
            print(f"- Datas de expiração únicas: {df_opcoes_final['expiracao'].nunique()}")
            ativos_count = df_opcoes_final.groupby('ativo_principal').size().sort_values(ascending=False)
            print(f"- Top 5 ativos principais com mais opções:")
            for ativo, count in ativos_count.head().items():
                print(f"  {ativo}: {count} pares")
            print(f"\n🔍 Primeiros 5 pares call/put salvos:")
            print(df_opcoes_final.head().to_string(index=False))
        return True
    except Exception as e:
        print(f"Erro ao processar e salvar opções: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("=== Exportador de Símbolos MetaTrader 5 ===")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    caminho_arquivo_strikes = os.path.join(script_dir, "SI_D_SEDE", "SI_D_SEDE.txt")
    print(f"Tentando carregar strikes do arquivo: {caminho_arquivo_strikes}")
    strikes_externos = carregar_strikes_externos(caminho_arquivo_strikes)
    if not strikes_externos:
        print("⚠️  Não foi possível carregar os strikes externos. O script prosseguirá usando os strikes do MT5 se disponíveis, mas o ajuste com dados externos não ocorrerá.")
    print("\nConectando ao MetaTrader 5...")
    if not conectar_mt5():
        return
    try:
        print("\nObtendo lista de símbolos do MT5...")
        simbolos_mt5 = obter_todos_simbolos()
        if simbolos_mt5 is None:
            return
        print("Processando informações dos símbolos do MT5...")
        dados_processados_mt5 = processar_simbolos(simbolos_mt5)
        nome_arquivo_saida_opcoes = "base.csv"
        print("\nProcessando e salvando opções call/put com strikes ajustados e filtro de data...")
        sucesso = salvar_csv_opcoes(dados_processados_mt5, nome_arquivo_saida_opcoes, strikes_externos)
        if sucesso:
            print("\n✅ Processo concluído com sucesso!")
        else:
            print("\n⚠️  Processo concluído, mas nenhum par call/put foi encontrado ou salvo, ou houve erro no processamento de opções.")
    except Exception as e:
        print(f"Erro durante a execução principal: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Desligando MT5 (se inicializado)...")
        mt5.shutdown()
        print("Conexão com MT5 encerrada.")

if __name__ == "__main__":
    main()

#--- END OF FILE sync.py ---