import tkinter as tk
from tkinter import ttk, messagebox, font as tkfont
import pandas as pd
import numpy as np
import MetaTrader5 as mt5
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import matplotlib.ticker as mtick
from datetime import datetime
import time
import json
import os
import subprocess 
import threading  
import sys        
import zipfile
import ctypes
from ctypes import wintypes

CSV_FILE_PATH = 'base.csv'
APP_TITLE = "Vector Profit Strategy"
HIGHLIGHT_COLOR = 'lightblue'
SETTINGS_FILE = "app_settings.json"
POSITION_FILES = {'M': 'position_m.json', 'R': 'position_r.json'}
FISCAL_M_FILE = 'fiscal_m.json'
FISCAL_R_FILE = 'fiscal_r.json'
TARGET_FONT = ('MS Reference Sans Serif', 8)
TARGET_FONT_BOLD = ('MS Reference Sans Serif', 8,'bold')
EVENT_DEBOUNCE_MS = 300

def setup_taskbar_icon():
    """Configura o ícone para aparecer corretamente na barra de tarefas do Windows"""
    try:
        # import ctypes # Imports já estão no topo do arquivo
        # from ctypes import wintypes
        
        # Define um ID único para a aplicação
        app_id = 'VectorProfit.OptionStrategy.1.0'
        
        # Tenta diferentes métodos para garantir que funcione
        try:
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(app_id)
        except Exception: # Captura genérica para evitar falha se shell32 não estiver disponível ou outros erros
            pass
            
        # Método alternativo usando kernel32
        try:
            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleTitleW(APP_TITLE) # Usar SetConsoleTitleW para unicode
        except Exception: # Captura genérica
            pass
            
        return True
    except Exception as e:
        print(f"Erro ao configurar ícone da barra de tarefas: {e}")
        return False

def mt5_connect():
    if not mt5.initialize():
        messagebox.showerror("Erro MT5", f"Falha ao inicializar MetaTrader 5: {mt5.last_error()}")
        return False
    print("Conectado ao MT5.")
    return True

def mt5_disconnect():
    mt5.shutdown()
    print("Desconectado do MT5.")

def mt5_get_all_prices_optimized(symbols_to_fetch):
    prices = {}
    if not symbols_to_fetch: return prices
    
    unique_symbols = list(filter(None, set(symbols_to_fetch)))
    if not unique_symbols: return prices

    for symbol in unique_symbols:
        if not mt5.symbol_select(symbol, True):
            print(f"Aviso: Não foi possível selecionar {symbol} no Market Watch via API.")
    time.sleep(0.2)
    
    symbol_ticks = {symbol: mt5.symbol_info_tick(symbol) for symbol in unique_symbols}
    
    def get_price(tick, price_type='ask'):
        if not tick: return None
        if price_type == 'ask': return tick.ask if tick.ask > 0 else (tick.last if tick.last > 0 else None)
        elif price_type == 'bid': return tick.bid if tick.bid > 0 else (tick.last if tick.last > 0 else None)
        return None

    for symbol in unique_symbols:
        tick_info = symbol_ticks.get(symbol)
        prices[f"{symbol}_ask"] = get_price(tick_info, 'ask')
        prices[f"{symbol}_bid"] = get_price(tick_info, 'bid')
        
    return prices

def mt5_get_symbol_price(symbol_name):
    prices = mt5_get_all_prices_optimized([symbol_name])
    price = prices.get(f'{symbol_name}_ask')
    if price is None: messagebox.showwarning("Erro MT5", f"Não foi possível obter preço de COMPRA (ASK) para {symbol_name}.")
    return price

class OptionStrategyApp:
    def __init__(self, root_window):
        self.B3_STOCK_NEGOTIATION_FEE_RATE = 0.00005
        self.B3_STOCK_SETTLEMENT_FEE_RATE = 0.00025
        self.B3_OPTIONS_SETTLEMENT_FEE_RATE = 0.000275
        self.B3_OPTIONS_REGISTRATION_FEE_RATE = 0.000695
        self.ISS_RATE = 0.05

        self.root = root_window
        self.root.title(APP_TITLE)
        self.root.geometry("1200x750")

        # CRÍTICO: Configurar antes de qualquer outra coisa
        setup_taskbar_icon()

        # Forçar aparição na barra de tarefas
        self.root.withdraw()  # Esconde temporariamente
        self.root.update_idletasks()
        self.root.deiconify()  # Mostra novamente
        self.root.focus_force()

        self.df_options, self.current_asset_price, self.selected_option_pair = None, None, None
        self.mt5_prices, self.current_position, self.tree_item_map = {}, {}, {}
        self.ax_left, self.ax_right = None, None
        self.current_position_key = 'T'
        self._tree_sort_column, self._tree_sort_reverse = None, False
        self.last_filtered_df_for_treeview = pd.DataFrame()
        self._debounce_job = None
        self._goal_seek_debounce_job = None
        self.last_graph_pnl_pct_sim = 0.0
        self.last_graph_pnl_pct_pos = 0.0

        self.qty_spinboxes = {}
        self.price_entries = {}
        self.unwind_qty_spinboxes = {}

        self.target_profit_var = tk.StringVar(value="40000")
        self.target_profit_pct_var = tk.StringVar(value="0.0")
        self._is_updating_target_profit = False
        self._target_profit_pct_debounce_job = None

        self.target_profit_var.trace_add("write", self._update_target_profit_pct)

        self.goal_seek_target_var = tk.StringVar(value="2500")

        self._setup_styles_and_plots()
        if not mt5_connect():
            self.root.destroy(); return
        self.load_data()
        self.create_widgets()
        self.load_settings()
        self.load_position_view(self.current_position_key)
        self.auto_load_initial_asset()
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Último recurso para forçar aparição
        self.root.after(100, self._force_taskbar_icon)

    def _get_current_assembly_cost(self):
        if not self.current_position:
            return 0
        pos = self.current_position
        cost = -(pos.get('asset_p', 0) * pos.get('asset_q', 0)) + \
               (pos.get('call_p', 0) * pos.get('call_q', 0)) - \
               (pos.get('put_p', 0) * pos.get('put_q', 0))
        return abs(cost)

    def trigger_target_profit_update_from_pct(self, event=None):
        if self._target_profit_pct_debounce_job:
            self.root.after_cancel(self._target_profit_pct_debounce_job)
        self._target_profit_pct_debounce_job = self.root.after(EVENT_DEBOUNCE_MS, self._update_target_profit_from_pct)

    def _update_target_profit_from_pct(self):
        if self._is_updating_target_profit:
            return
        self._is_updating_target_profit = True
        
        try:
            pct_val = float(self.target_profit_pct_var.get().replace(',', '.')) / 100.0
            custo_montagem = self._get_current_assembly_cost()
            if custo_montagem > 0:
                new_target_value = custo_montagem * pct_val
                self.target_profit_var.set(f"{new_target_value:.0f}")
        except (ValueError, TypeError):
            pass
        finally:
            self._is_updating_target_profit = False

    def _update_target_profit_pct(self, *args):
        if self._is_updating_target_profit:
            return
        self._is_updating_target_profit = True

        try:
            target_val = float(self.target_profit_var.get().replace(',', '.'))
            custo_montagem = self._get_current_assembly_cost()
            if custo_montagem > 0:
                new_pct_value = (target_val / custo_montagem) * 100
                self.target_profit_pct_var.set(f"{new_pct_value:.2f}")
            else:
                self.target_profit_pct_var.set("0.00")
        except (ValueError, TypeError):
            self.target_profit_pct_var.set("N/A")
        finally:
            self._is_updating_target_profit = False

    def _setup_styles_and_plots(self):
        try: self.root.iconbitmap('icon.ico')
        except tk.TclError: print("Aviso: 'icon.ico' não encontrado.")
        style = ttk.Style()
        style.configure("TLabel", font=TARGET_FONT)
        style.configure("TButton", font=TARGET_FONT)
        style.configure("Treeview", font=TARGET_FONT, rowheight=18)
        style.configure("Treeview.Heading", font=TARGET_FONT_BOLD)
        style.configure("TLabelframe.Label", font=TARGET_FONT_BOLD)
        style.configure("TCombobox", font=TARGET_FONT)
        
        style.configure("NoBorder.Treeview", borderwidth=0, relief="flat", rowheight=18, font=TARGET_FONT)
        style.layout("NoBorder.Treeview", [('Treeview.treearea', {'sticky': 'nswe'})])
        style.configure("NoBorder.Treeview.Heading", font=TARGET_FONT)
        
        # Configurar ícone PRIMEIRO
        try: 
            self.root.iconbitmap('icon.ico')
            # Forçar atualização
            self.root.update()
            self.root.lift()
            self.root.attributes('-topmost', True)
            self.root.after_idle(self.root.attributes, '-topmost', False)
        except tk.TclError: 
            print("Aviso: 'icon.ico' não encontrado.")

        self.root.option_add("*TCombobox*Listbox*Font", TARGET_FONT)
        plt.rcParams.update({'font.size': 9, 'axes.titlesize': 8,'font.family': 'MS Reference Sans Serif'})

    def load_data(self):
        try:
            self.df_options = pd.read_csv(CSV_FILE_PATH, sep=';')
            self.df_options['strike'] = pd.to_numeric(self.df_options['strike'].str.replace(',', '.'), errors='coerce')
            self.df_options.dropna(subset=['strike'], inplace=True)
        except Exception as e:
            messagebox.showerror("Erro de Arquivo", f"Arquivo {CSV_FILE_PATH} não encontrado ou inválido: {e}")
            self.df_options = pd.DataFrame(); self.root.destroy(); return

    def on_closing(self):
        self.save_settings()
        mt5_disconnect()
        self.root.quit()
        self.root.destroy()
        import sys as sys_on_close
        sys_on_close.exit(0)

    def _read_single_position_file(self, file_key):
        filename = POSITION_FILES.get(file_key)
        if not filename or not os.path.exists(filename):
            return {}
        try:
            with open(filename, "r") as f:
                pos_data = json.load(f)
            return pos_data if isinstance(pos_data, dict) and pos_data else {}
        except (json.JSONDecodeError, IOError):
            return {}
            
    def _combine_positions(self, pos_m, pos_r):
        if not pos_m and not pos_r: return {}
        if not pos_m: return pos_r
        if not pos_r: return pos_m

        if pos_m.get('tickers', {}).get('asset') != pos_r.get('tickers', {}).get('asset'):
            messagebox.showwarning("Inconsistência", "As posições 'M' e 'R' são de ativos principais diferentes. A combinação pode não ser precisa.")

        combined = {}
        combined['tickers'] = pos_m.get('tickers') or pos_r.get('tickers')
        combined['expiracao'] = pos_m.get('expiracao') or pos_r.get('expiracao')
        combined['strike'] = pos_m.get('strike') or pos_r.get('strike')

        for leg in ['asset', 'call', 'put']:
            q_m = pos_m.get(f'{leg}_q', 0)
            p_m = pos_m.get(f'{leg}_p', 0)
            q_r = pos_r.get(f'{leg}_q', 0)
            p_r = pos_r.get(f'{leg}_p', 0)
            
            total_q = q_m + q_r
            
            if total_q == 0:
                avg_p = 0
            else:
                avg_p = ((p_m * q_m) + (p_r * q_r)) / total_q
            
            combined[f'{leg}_q'] = total_q
            combined[f'{leg}_p'] = avg_p
            
        return combined

    def load_position_view(self, view_key):
        self.current_position_key = view_key
        
        if view_key == 'T':
            pos_m = self._read_single_position_file('M')
            pos_r = self._read_single_position_file('R')
            self.current_position = self._combine_positions(pos_m, pos_r)
        else:
            self.current_position = self._read_single_position_file(view_key)

        if hasattr(self, 'position_frame'):
            self.position_frame.config(text=f"Posição Atual ({self.current_position_key})")

        self._update_button_states()
        self._update_all_dynamic_info()
        self._populate_unwind_boxes_from_position()
    
    def _update_button_states(self):
        is_read_only_view = (self.current_position_key == 'T')
        write_state = tk.DISABLED if is_read_only_view else tk.NORMAL
        
        if hasattr(self, 'assemble_btn'):
            self.assemble_btn.config(state=write_state)
            self.reset_btn.config(state=write_state)
            self.copy_pos_btn.config(state=tk.NORMAL)

    def save_current_position(self):
        if self.current_position_key in POSITION_FILES:
            filename = POSITION_FILES[self.current_position_key]
            try:
                with open(filename, "w") as f:
                    json.dump(self.current_position, f, indent=4)
            except IOError as e:
                messagebox.showerror("Erro ao Salvar", f"Não foi possível salvar a posição em {filename}: {e}")

    def reset_position(self):
        if self.current_position_key == 'T': return
        if not self.current_position: messagebox.showinfo("Zerar Posição", "Nenhuma posição montada para zerar."); return
        if messagebox.askyesno("Confirmar", f"Tem certeza que deseja zerar a posição '{self.current_position_key}'?"):
            self.current_position = {}
            self.save_current_position()
            self.load_position_view(self.current_position_key)
            messagebox.showinfo("Sucesso", "Posição zerada com sucesso.")

    def assemble_position(self):
        if self.current_position_key == 'T': return
        new_op_params = self._get_strategy_parameters()
        if not new_op_params: messagebox.showerror("Erro", "Dados da operação inválidos."); return
        if not self.selected_option_pair: messagebox.showerror("Erro", "Nenhum par de opções selecionado."); return

        new_tickers = {'asset': self.selected_option_pair['ativo_principal'], 'call': self.selected_option_pair['ticker_call'], 'put': self.selected_option_pair['ticker_put']}
        new_exp_date = self.selected_option_pair['expiracao']

        if not self.current_position:
            self.current_position = {
                'tickers': new_tickers, 'expiracao': new_exp_date, 'asset_q': new_op_params['asset_q'], 'asset_p': new_op_params['asset_p'],
                'call_q': new_op_params['call_q'], 'call_p': new_op_params['call_p'], 'put_q': new_op_params['put_q'], 'put_p': new_op_params['put_p'],
                'strike': new_op_params['strike']
            }
        else:
            if self.current_position.get('tickers',{}).get('asset') != new_tickers['asset']:
                 messagebox.showerror("Erro", "Não é possível adicionar a uma posição com ativo principal diferente."); return
            total_asset_q = self.current_position.get('asset_q', 0) + new_op_params['asset_q']
            total_call_q = self.current_position.get('call_q', 0) + new_op_params['call_q']
            total_put_q = self.current_position.get('put_q', 0) + new_op_params['put_q']
            
            self.current_position['asset_p'] = ((self.current_position.get('asset_p', 0) * self.current_position.get('asset_q', 0)) + (new_op_params['asset_p'] * new_op_params['asset_q'])) / total_asset_q if total_asset_q != 0 else 0
            self.current_position['call_p'] = ((self.current_position.get('call_p', 0) * self.current_position.get('call_q', 0)) + (new_op_params['call_p'] * new_op_params['call_q'])) / total_call_q if total_call_q != 0 else 0
            self.current_position['put_p'] = ((self.current_position.get('put_p', 0) * self.current_position.get('put_q', 0)) + (new_op_params['put_p'] * new_op_params['put_q'])) / total_put_q if total_put_q != 0 else 0
            
            self.current_position.update({
                'tickers': new_tickers, 'expiracao': new_exp_date, 'asset_q': total_asset_q, 'call_q': total_call_q, 'put_q': total_put_q,
                'strike': new_op_params['strike']
            })
        
        self.save_current_position()
        self._update_all_dynamic_info()
        self._populate_unwind_boxes_from_position()
        messagebox.showinfo("Sucesso", "Operação adicionada à posição!")

    def _update_all_dynamic_info(self):
        self.update_position_display()
        self.calculate_and_display_rollover()
        self._update_payout_graphs()
        
    def _populate_unwind_boxes_from_position(self):
        if self.current_position:
            self.unwind_qty_spinboxes["Ações"]["var"].set(self.current_position.get('asset_q', 0))
            self.unwind_qty_spinboxes["Calls"]["var"].set(self.current_position.get('call_q', 0))
            self.unwind_qty_spinboxes["Puts"]["var"].set(self.current_position.get('put_q', 0))
        else:
            self.unwind_qty_spinboxes["Ações"]["var"].set(0)
            self.unwind_qty_spinboxes["Calls"]["var"].set(0)
            self.unwind_qty_spinboxes["Puts"]["var"].set(0)

    def save_settings(self):
        settings = {
            "selected_asset": self.asset_combo.get() if hasattr(self, 'asset_combo') else "",
            "active_position_key": self.current_position_key
        }
        try:
            if self.root.state() != 'zoomed': settings["window_geometry"] = self.root.winfo_geometry()
            settings["window_state"] = self.root.state()
            if hasattr(self, 'main_paned_window'): settings["main_pane_sash"] = self.main_paned_window.sashpos(0)
            if hasattr(self, 'right_vertical_pane'): settings["right_vertical_sash"] = self.right_vertical_pane.sashpos(0)
            if hasattr(self, 'tree'): settings["treeview_columns"] = {col_id: self.tree.column(col_id, "width") for col_id in self.tree["columns"]}
            # Salvar as posições das novas divisórias
            if hasattr(self, 'bottom_paned_window'): settings["bottom_pane_sash_1"] = self.bottom_paned_window.sashpos(0)
            if hasattr(self, 'inner_bottom_paned_window'): settings["bottom_pane_sash_2"] = self.inner_bottom_paned_window.sashpos(0)
        except tk.TclError: pass
        with open(SETTINGS_FILE, "w") as f: json.dump(settings, f, indent=4)

    def load_settings(self):
        try:
            with open(SETTINGS_FILE, "r") as f: settings = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError): settings = {}
        
        self.current_position_key = settings.get("active_position_key", "T")

        if settings.get("window_state") == 'zoomed':
            try: self.root.state('zoomed')
            except tk.TclError: self.root.attributes('-zoomed', True)
        elif "window_geometry" in settings: self.root.geometry(settings["window_geometry"])
        self.asset_combo.set(settings.get("selected_asset", "PETR4"))
        self.root.update_idletasks()
        # Alterado de after_idle para after(100) conforme a solução do usuário
        self.root.after(100, self._apply_layout_settings, settings)

    def _apply_layout_settings(self, settings):
        if not settings: return
        sash_map = { 
            "main_pane_sash": self.main_paned_window, 
            "right_vertical_sash": self.right_vertical_pane,
            # Restaurar as posições das novas divisórias
            "bottom_pane_sash_1": self.bottom_paned_window if hasattr(self, 'bottom_paned_window') else None,
            "bottom_pane_sash_2": self.inner_bottom_paned_window if hasattr(self, 'inner_bottom_paned_window') else None
        }
        for key, pane in sash_map.items():
            if pane and key in settings and hasattr(pane, 'winfo_exists') and pane.winfo_exists():
                sash_value = settings[key]
                if isinstance(sash_value, (int, float)) and sash_value > 10: # Validação adicionada
                    try:
                        pane.sashpos(0, int(sash_value))
                    except tk.TclError:
                        pass # Erro ao aplicar sashpos é ignorado

        if "treeview_columns" in settings and self.tree.winfo_exists():
            for col_id, width in settings["treeview_columns"].items():
                if col_id in self.tree["columns"]: self.tree.column(col_id, width=width)
    
    def create_widgets(self):
        self.main_paned_window = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        self.main_paned_window.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        left_frame = ttk.Frame(self.main_paned_window, width=400, height=730)
        left_frame.pack_propagate(False)
        self.main_paned_window.add(left_frame, weight=1)
        
        filter_frame = ttk.LabelFrame(left_frame, text="Filtro de Ativo")
        filter_frame.pack(padx=5, pady=5, fill=tk.X)
        ttk.Label(filter_frame, text="Ativo Principal:").grid(row=0, column=0, padx=5, pady=2, sticky=tk.W)
        self.asset_combo = ttk.Combobox(filter_frame, width=15, values=sorted(self.df_options['ativo_principal'].astype(str).unique()))
        self.asset_combo.grid(row=0, column=1, padx=5, pady=2, sticky=tk.EW)
        self.asset_combo.bind("<<ComboboxSelected>>", self.on_asset_selected)
        self.asset_combo.bind("<Return>", lambda e: self.on_asset_selected())
        filter_frame.columnconfigure(1, weight=1)
        
        tree_frame = ttk.LabelFrame(left_frame, text="Pares de Opções Filtradas")
        tree_frame.pack(padx=5, pady=5, fill=tk.BOTH, expand=True)
        cols = ('ticker_call', 'ticker_put', 'strike', 'expiracao')
        self.tree = ttk.Treeview(tree_frame, columns=cols, show='headings', selectmode='browse')
        col_widths = {'ticker_call': 90, 'ticker_put': 90, 'strike': 70, 'expiracao': 80}
        for col in cols:
            self.tree.heading(col, text=col.replace('_', ' ').title(), command=lambda c=col: self.sort_treeview_column(c))
            self.tree.column(col, width=col_widths.get(col, 80), anchor=tk.CENTER, stretch=tk.YES)
        self.tree.bind("<<TreeviewSelect>>", self.on_tree_select)
        self.tree.tag_configure('closest_strike', background=HIGHLIGHT_COLOR, foreground='black')
        vsb, hsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview), ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.tree.pack(fill=tk.BOTH, expand=True)
        
        op_frame = ttk.LabelFrame(left_frame, text="Operação / %Rolagem")
        op_frame.pack(padx=5, pady=5, fill=tk.X, side=tk.BOTTOM)
        op_items = {"Ações": "1000", "Calls": "1000", "Puts": "1000"}
        for i, (key, default_qty) in enumerate(op_items.items()):
            op_frame.columnconfigure(i, weight=1)
            ttk.Label(op_frame, text=f"{key}:").grid(row=0, column=i, padx=5, pady=(3, 0))
            qty_var = tk.StringVar(value=default_qty)
            qty_widget = tk.Spinbox(op_frame, from_=0, to=10000000, textvariable=qty_var, increment=100, command=self.trigger_recalculation, font=TARGET_FONT, relief=tk.FLAT)
            qty_widget.grid(row=1, column=i, padx=5, pady=(2, 0), sticky='ew')
            qty_widget.bind("<KeyRelease>", self.trigger_recalculation); qty_widget.bind("<FocusOut>", self.trigger_recalculation); qty_widget.bind("<Return>", self.trigger_recalculation)
            self.qty_spinboxes[key] = {"widget": qty_widget, "var": qty_var}
        for i, key in enumerate(op_items.keys()):
            price_var = tk.StringVar()
            price_widget = tk.Entry(op_frame, textvariable=price_var, font=TARGET_FONT, relief=tk.FLAT, justify=tk.CENTER)
            price_widget.grid(row=2, column=i, padx=5, pady=(0, 3), sticky='ew')
            price_widget.bind("<KeyRelease>", self.trigger_recalculation); price_widget.bind("<FocusOut>", self.trigger_recalculation); price_widget.bind("<Return>", self.trigger_recalculation)
            self.price_entries[key] = {"widget": price_widget, "var": price_var}

        right_main_frame = ttk.Frame(self.main_paned_window)
        self.main_paned_window.add(right_main_frame, weight=3)
        
        self.right_vertical_pane = ttk.PanedWindow(right_main_frame, orient=tk.VERTICAL)
        self.right_vertical_pane.pack(fill=tk.BOTH, expand=True)

        graph_frame = ttk.LabelFrame(self.right_vertical_pane, text="Gráficos de Payout")
        self.fig, (self.ax_left, self.ax_right) = plt.subplots(1, 2, sharex=True, sharey=True, figsize=(10, 4),gridspec_kw={'width_ratios': [1, 1]})
        self.canvas = FigureCanvasTkAgg(self.fig, master=graph_frame)
        self.canvas.draw()
        
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.right_vertical_pane.add(graph_frame, weight=2)
        
        # --- INÍCIO DA ÁREA MODIFICADA COM PANEDWINDOW ---
        bottom_text_container = ttk.Frame(self.right_vertical_pane)
        self.right_vertical_pane.add(bottom_text_container, weight=1)

        self.bottom_paned_window = ttk.PanedWindow(bottom_text_container, orient=tk.HORIZONTAL)
        self.bottom_paned_window.pack(fill=tk.BOTH, expand=True)

        # Frame da Montagem (primeiro painel)
        montagem_frame = ttk.LabelFrame(self.bottom_paned_window, text="Montagem")
        self.bottom_paned_window.add(montagem_frame) # Peso 1 REMOVIDO

        # --- Nova estrutura para Montagem ---
        montagem_frame.columnconfigure(0, weight=1) # Permitir que a treeview expanda

        # Parte Superior (Textos)
        self.montagem_tickers_label = ttk.Label(montagem_frame, text="", font=TARGET_FONT_BOLD)
        self.montagem_tickers_label.grid(row=0, column=0, sticky="ew", padx=7, pady=(5, 5)) # Adjusted pady

        # Parte Intermediária (Treeview)
        montagem_tree_frame = ttk.Frame(montagem_frame) # Frame para conter a treeview e scrollbar
        montagem_tree_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=(0,5)) # Adjusted row from 2 to 1
        montagem_tree_frame.rowconfigure(0, weight=1)
        montagem_tree_frame.columnconfigure(0, weight=1)
        
        montagem_cols = ('key', 'value')
        self.montagem_details_tree = ttk.Treeview(montagem_tree_frame, columns=montagem_cols, show='', style="NoBorder.Treeview", selectmode='none', height=7) # Definir altura inicial, show changed to ''
        self.montagem_details_tree.grid(row=0, column=0, sticky="nsew")
        
        # self.montagem_details_tree.heading('key', text='Info') # Headers no longer shown
        # self.montagem_details_tree.heading('value', text='Valor') # Headers no longer shown
        self.montagem_details_tree.column('key', width=100, anchor=tk.W, stretch=tk.NO) # Coluna da chave não estica inicialmente
        self.montagem_details_tree.column('value', width=150, anchor=tk.W, stretch=tk.YES) # Coluna do valor estica

        montagem_frame.rowconfigure(1, weight=1) # Adjusted row from 2 to 1: Permitir que a treeview expanda verticalmente

        # Parte Inferior (Textos D+1, D+2)
        self.montagem_settlement_frame = ttk.Frame(montagem_frame)
        self.montagem_settlement_frame.grid(row=2, column=0, sticky="ew", padx=7, pady=(5, 5)) # Adjusted row from 3 to 2
        self.montagem_settlement_frame.columnconfigure(1, weight=1) # Para o valor expandir se necessário

        ttk.Label(self.montagem_settlement_frame, text="D+1:").grid(row=0, column=0, sticky=tk.W)
        self.montagem_d1_value_label = ttk.Label(self.montagem_settlement_frame, text="R$ 0.00", font=TARGET_FONT)
        self.montagem_d1_value_label.grid(row=0, column=1, sticky=tk.EW, padx=(5,0))
        
        ttk.Label(self.montagem_settlement_frame, text="D+2:").grid(row=1, column=0, sticky=tk.W, pady=(2,0))
        self.montagem_d2_value_label = ttk.Label(self.montagem_settlement_frame, text="R$ 0.00", font=TARGET_FONT)
        self.montagem_d2_value_label.grid(row=1, column=1, sticky=tk.EW, padx=(5,0), pady=(2,0))
        # --- Fim da Nova estrutura para Montagem ---

        # Container para os outros dois painéis
        right_sub_pane = ttk.PanedWindow(self.bottom_paned_window, orient=tk.HORIZONTAL)
        self.bottom_paned_window.add(right_sub_pane) # Peso 2 (dobro da montagem) REMOVIDO
        self.inner_bottom_paned_window = right_sub_pane # Salvar referência

        # Frame da Rolagem (dentro do sub-painel)
        rolagem_frame = ttk.LabelFrame(right_sub_pane, text="Rolagem")
        right_sub_pane.add(rolagem_frame) # Peso 2 REMOVIDO

        rolagem_frame.rowconfigure(1, weight=1)
        rolagem_frame.columnconfigure(0, weight=1)
        self.rolagem_header_label = ttk.Label(rolagem_frame, text="", font=TARGET_FONT_BOLD)
        self.rolagem_header_label.grid(row=0, column=0, sticky="ew", padx=7, pady=(5, 2))
        trades_cols = ('operation', 'quantity', 'price', 'financial')
        self.rolagem_trades_tree = ttk.Treeview(rolagem_frame, columns=trades_cols, show='', style="NoBorder.Treeview", selectmode='none')
        self.rolagem_trades_tree.grid(row=1, column=0, sticky="nsew", padx=5)
        self.rolagem_trades_tree.column("#0", width=0, stretch=tk.NO)
        self.rolagem_trades_tree.column("operation", width=110, anchor=tk.W)
        self.rolagem_trades_tree.column("quantity", width=65, anchor=tk.E)
        self.rolagem_trades_tree.column("price", width=55, anchor=tk.E)
        self.rolagem_trades_tree.column("financial", width=110, anchor=tk.E)
        self.rolagem_trades_tree.tag_configure('positivo', foreground='blue')
        self.rolagem_trades_tree.tag_configure('negativo', foreground='red')
        
        self.settlement_frame = ttk.Frame(rolagem_frame)
        self.settlement_frame.grid(row=2, column=0, sticky="ew", padx=7, pady=(5, 2))
        self.settlement_frame.columnconfigure(1, weight=1) # Para o valor expandir

        ttk.Label(self.settlement_frame, text="D+1:").grid(row=0, column=0, sticky=tk.W)
        self.d1_value_label = ttk.Label(self.settlement_frame, text="R$ 0.00", font=TARGET_FONT)
        self.d1_value_label.grid(row=0, column=1, sticky=tk.EW, padx=(5,0))
        
        ttk.Label(self.settlement_frame, text="D+2:").grid(row=1, column=0, sticky=tk.W, pady=(2,0))
        self.d2_value_label = ttk.Label(self.settlement_frame, text="R$ 0.00", font=TARGET_FONT)
        self.d2_value_label.grid(row=1, column=1, sticky=tk.EW, padx=(5,0), pady=(2,0))
        
        self.rolagem_footer_label = ttk.Label(rolagem_frame, text="", font=TARGET_FONT)
        self.rolagem_footer_label.grid(row=3, column=0, sticky="ew", padx=7, pady=(2, 5))
        self.rollover_context_menu = tk.Menu(rolagem_frame, tearoff=0)
        self.rollover_context_menu.add_command(label="Copiar basket", command=self.copy_rollover_basket_to_clipboard)
        rolagem_frame.bind("<Button-3>", self.show_rollover_context_menu)
        self.rolagem_trades_tree.bind("<Button-3>", self.show_rollover_context_menu)
        unwind_qty_frame = ttk.Frame(rolagem_frame)
        unwind_qty_frame.grid(row=4, column=0, sticky="ew", padx=0, pady=(5, 2))

        # Frame da Posição (dentro do sub-painel)
        self.position_frame = ttk.LabelFrame(right_sub_pane, text=f"Posição Atual ({self.current_position_key})")
        right_sub_pane.add(self.position_frame) # Peso 1 REMOVIDO

        # self.position_frame.rowconfigure(0, weight=1) # Old configuration
        self.position_frame.columnconfigure(0, weight=1) # Keep column config

        # New layout for position_frame:
        # Row 0: position_header_frame (tickers, cal_days, expiry_date)
        # Row 1: position_details_tree_frame (Asset/Call/Put details) - weight 1 for expansion
        # Row 2: position_summary_text (financial summary) - weight 1 for expansion
        # Row 3: position_alvo_custo_label
        # Row 4: position_action_frame (buttons)

        self.position_frame.rowconfigure(0, weight=0) 
        self.position_frame.rowconfigure(1, weight=1) # Treeview expands
        self.position_frame.rowconfigure(2, weight=1) # Summary text expands
        self.position_frame.rowconfigure(3, weight=0)
        self.position_frame.rowconfigure(4, weight=0)

        # Row 0: Header Frame
        position_header_frame = ttk.Frame(self.position_frame)
        position_header_frame.grid(row=0, column=0, sticky="ew", padx=5, pady=(5,5)) # Adjusted pady
        position_header_frame.columnconfigure(0, weight=1) # Ticker label expands

        self.position_tickers_display_label = ttk.Label(position_header_frame, text="", font=TARGET_FONT_BOLD)
        self.position_tickers_display_label.grid(row=0, column=0, sticky="ew") # Will now include days

        # self.position_cal_days_label = ttk.Label(position_header_frame, text="", font=TARGET_FONT) # Removed
        # self.position_cal_days_label.grid(row=0, column=1, sticky="e", padx=(5,0))
        
        # self.position_expiry_label = ttk.Label(position_header_frame, text="", font=TARGET_FONT) # Removed
        # self.position_expiry_label.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0,2))

        # Row 1: Position Details Treeview Frame
        position_details_tree_frame = ttk.Frame(self.position_frame)
        position_details_tree_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=0)
        position_details_tree_frame.rowconfigure(0, weight=1)
        position_details_tree_frame.columnconfigure(0, weight=1)

        pos_details_cols = ('instrumento', 'quantidade', 'preco_medio')
        self.position_details_tree = ttk.Treeview(position_details_tree_frame, columns=pos_details_cols, show='', style="NoBorder.Treeview", selectmode='none', height=3) # height=3 for 3 rows, show=''
        self.position_details_tree.grid(row=0, column=0, sticky="nsew")

        # self.position_details_tree.heading('instrumento', text='Instrumento') # Headings no longer shown
        # self.position_details_tree.heading('quantidade', text='Quantidade')
        # self.position_details_tree.heading('preco_medio', text='Preço Médio')

        self.position_details_tree.column('instrumento', width=120, anchor=tk.W, stretch=tk.NO)
        self.position_details_tree.column('quantidade', width=80, anchor=tk.W, stretch=tk.NO) # Changed anchor to tk.W
        self.position_details_tree.column('preco_medio', width=80, anchor=tk.W, stretch=tk.YES) # Changed anchor to tk.W

        # Row 2: Position Summary Text
        self.position_summary_text = tk.Text(self.position_frame, wrap=tk.WORD, font=TARGET_FONT, state=tk.DISABLED, borderwidth=0, relief="flat", height=5) # height=5 for 5 lines
        self.position_summary_text.grid(row=2, column=0, sticky="nsew", padx=5, pady=(2,2))
        self.position_summary_text.tag_config("positivo", foreground="blue", font=TARGET_FONT)
        self.position_summary_text.tag_config("negativo", foreground="red", font=TARGET_FONT)
        self.position_summary_text.tag_config("black_fg", foreground="black", font=TARGET_FONT)

        # Row 3: Alvo+Custo Label
        self.position_alvo_custo_label = ttk.Label(self.position_frame, text="", font=TARGET_FONT)
        self.position_alvo_custo_label.grid(row=3, column=0, sticky="ew", padx=7, pady=(0,5))

        # Row 4: Action Frame (Buttons)
        position_action_frame = ttk.Frame(self.position_frame)
        position_action_frame.grid(row=4, column=0, sticky='ew', pady=(5,5))

        # --- Contents of position_action_frame START ---
        action_buttons_frame = ttk.Frame(position_action_frame)
        action_buttons_frame.pack(fill=tk.X, expand=True, pady=(0, 5)) # pack into position_action_frame
        action_buttons_frame.columnconfigure((0, 1, 2), weight=1)

        self.assemble_btn = ttk.Button(action_buttons_frame, text="Montar/Adicionar", command=self.assemble_position)
        self.assemble_btn.grid(row=0, column=0, sticky='ew', padx=(5,2))

        self.reset_btn = ttk.Button(action_buttons_frame, text="Zerar Posição", command=self.reset_position)
        self.reset_btn.grid(row=0, column=1, sticky='ew', padx=2)

        self.copy_pos_btn = ttk.Button(action_buttons_frame, text="Copiar Posição", command=self.populate_assembly_from_current_position)
        self.copy_pos_btn.grid(row=0, column=2, sticky='ew', padx=(2,5))
        
        ttk.Separator(action_buttons_frame, orient='horizontal').grid(row=1, column=0, columnspan=3, sticky='ew', pady=(5, 2))

        load_btn_frame = ttk.Frame(action_buttons_frame) # Child of action_buttons_frame
        load_btn_frame.grid(row=2, column=0, columnspan=3, sticky='ew')
        load_btn_frame.columnconfigure(0, weight=1) # Centering setup
        load_btn_frame.columnconfigure(1, weight=0) # Centering setup - content
        load_btn_frame.columnconfigure(2, weight=1) # Centering setup

        centered_frame = ttk.Frame(load_btn_frame) # This frame will contain the buttons
        centered_frame.grid(row=0, column=1) # Place in the middle column of load_btn_frame

        ttk.Label(centered_frame, text="Visão:").pack(side=tk.LEFT, padx=(0, 5))
        m_btn = ttk.Button(centered_frame, text="M", width=4, command=lambda: self.load_position_view('M'))
        m_btn.pack(side=tk.LEFT)
        r_btn = ttk.Button(centered_frame, text="R", width=4, command=lambda: self.load_position_view('R'))
        r_btn.pack(side=tk.LEFT, padx=2)
        t_btn = ttk.Button(centered_frame, text="T", width=4, command=lambda: self.load_position_view('T'))
        t_btn.pack(side=tk.LEFT)
        
        fiscal_m_btn = ttk.Button(centered_frame, text="Fs M",width=5, command=lambda: self.show_fiscal_report_popup(FISCAL_M_FILE, "Relatório Fiscal M"))
        fiscal_m_btn.pack(side=tk.LEFT, padx=(5,0))
        fiscal_r_btn = ttk.Button(centered_frame, text="Fs R",width=5, command=lambda: self.show_fiscal_report_popup(FISCAL_R_FILE, "Relatório Fiscal R"))
        fiscal_r_btn.pack(side=tk.LEFT, padx=(2,0))

        self.sync_btn = ttk.Button(centered_frame, text="Sy",width=5, command=self.run_sync_scripts)
        self.sync_btn.pack(side=tk.LEFT, padx=(5,0))

        self.si_btn = ttk.Button(centered_frame, text="SI", width=5, command=self.run_si_extraction)
        self.si_btn.pack(side=tk.LEFT, padx=(2,0))
        
        advanced_goal_seek_frame = ttk.Frame(position_action_frame) # Child of position_action_frame
        advanced_goal_seek_frame.pack(fill=tk.X, expand=True, padx=5) # pack into position_action_frame
        
        ttk.Label(advanced_goal_seek_frame, text="Alvo:").grid(row=0, column=0, sticky='w', padx=(0,5))
        target_profit_entry = tk.Entry(advanced_goal_seek_frame, textvariable=self.target_profit_var, font=TARGET_FONT, width=10, justify=tk.RIGHT)
        target_profit_entry.grid(row=0, column=1, sticky='w')

        ttk.Label(advanced_goal_seek_frame, text="%:").grid(row=0, column=2, sticky='w', padx=(10, 5))
        target_profit_pct_entry = tk.Entry(advanced_goal_seek_frame, textvariable=self.target_profit_pct_var, font=TARGET_FONT, width=7, justify=tk.RIGHT)
        target_profit_pct_entry.grid(row=0, column=3, sticky='w')
        target_profit_pct_entry.bind("<KeyRelease>", self.trigger_target_profit_update_from_pct)
        target_profit_pct_entry.bind("<Return>", self.trigger_target_profit_update_from_pct)

        calc_button = ttk.Button(advanced_goal_seek_frame, text="Calcular Rolagem", command=self.calculate_rollover_for_target_profit)
        calc_button.grid(row=0, column=4, sticky='ew', padx=(10,0))
        advanced_goal_seek_frame.columnconfigure(4, weight=1)
        # --- Contents of position_action_frame END ---

        # --- FIM DA ÁREA MODIFICADA COM PANEDWINDOW --- (This comment seems to be a marker for the end of the right_sub_pane)

        op_items_unwind = {"Ações": "0", "Calls": "0", "Puts": "0"}
        
        col_idx = 0
        for key, default_qty in op_items_unwind.items():
            ttk.Label(unwind_qty_frame, text=key).grid(row=0, column=col_idx, padx=2, sticky='s', pady=(0,1))
            qty_var = tk.StringVar(value=default_qty)
            qty_widget = tk.Spinbox(unwind_qty_frame, from_=0, to=10000000, textvariable=qty_var, increment=100, command=self.trigger_recalculation, font=TARGET_FONT, relief=tk.FLAT, width=7)
            qty_widget.grid(row=1, column=col_idx, padx=(5 if col_idx == 0 else 2, 2), sticky='n')
            qty_widget.bind("<KeyRelease>", self.trigger_recalculation)
            self.unwind_qty_spinboxes[key] = {"widget": qty_widget, "var": qty_var}
            col_idx += 1
            
        ttk.Label(unwind_qty_frame, text="Meta").grid(row=0, column=col_idx, padx=2, sticky='s', pady=(0,1))
        
        goal_seek_spinbox = tk.Spinbox(
            unwind_qty_frame, 
            from_=-1000000, to=1000000,
            increment=200,
            textvariable=self.goal_seek_target_var, 
            font=TARGET_FONT, 
            width=7,
            relief=tk.FLAT,
            command=self.trigger_goal_seek,
            justify=tk.RIGHT
        )
        goal_seek_spinbox.grid(row=1, column=col_idx, padx=2, sticky='n')
        goal_seek_spinbox.bind("<KeyRelease>", self.trigger_goal_seek)
        
        # position_action_frame will be recreated and placed at row 4 later in this step
        # ttk.Separator(action_buttons_frame, orient='horizontal').grid(row=1, column=0, columnspan=3, sticky='ew', pady=(5, 2))
        # load_btn_frame = ttk.Frame(action_buttons_frame)
        # load_btn_frame.grid(row=2, column=0, columnspan=3, sticky='ew')
        # ... (button recreation will be part of adding position_action_frame)
        
        # advanced_goal_seek_frame will be part of the new position_action_frame
        # advanced_goal_seek_frame = ttk.Frame(position_action_frame)
        # advanced_goal_seek_frame.pack(fill=tk.X, expand=True, padx=5)
        # ttk.Label(advanced_goal_seek_frame, text="Alvo:").grid(row=0, column=0, sticky='w', padx=(0,5))
        target_profit_entry = tk.Entry(advanced_goal_seek_frame, textvariable=self.target_profit_var, font=TARGET_FONT, width=10, justify=tk.RIGHT)
        target_profit_entry.grid(row=0, column=1, sticky='w')

        ttk.Label(advanced_goal_seek_frame, text="%:").grid(row=0, column=2, sticky='w', padx=(10, 5))
        target_profit_pct_entry = tk.Entry(advanced_goal_seek_frame, textvariable=self.target_profit_pct_var, font=TARGET_FONT, width=7, justify=tk.RIGHT)
        target_profit_pct_entry.grid(row=0, column=3, sticky='w')
        target_profit_pct_entry.bind("<KeyRelease>", self.trigger_target_profit_update_from_pct)
        target_profit_pct_entry.bind("<Return>", self.trigger_target_profit_update_from_pct)

        calc_button = ttk.Button(advanced_goal_seek_frame, text="Calcular Rolagem", command=self.calculate_rollover_for_target_profit)
        calc_button.grid(row=0, column=4, sticky='ew', padx=(10,0))
        advanced_goal_seek_frame.columnconfigure(4, weight=1)

        self.clear_plots()

    # O restante do código (métodos de cálculo, etc.) permanece o mesmo...
    # ... cole todo o restante do seu código a partir daqui ...
    def trigger_recalculation(self, event=None):
        if self._debounce_job: self.root.after_cancel(self._debounce_job)
        self._debounce_job = self.root.after(EVENT_DEBOUNCE_MS, self.on_input_change)

    def trigger_goal_seek(self, event=None):
        if self._goal_seek_debounce_job:
            self.root.after_cancel(self._goal_seek_debounce_job)
        self._goal_seek_debounce_job = self.root.after(EVENT_DEBOUNCE_MS, self.perform_d2_goal_seek)

    def on_input_change(self):
        self._update_payout_graphs()
        
        params = self._get_strategy_parameters()
        if params:
            self._update_summary_widgets(params)
        else:
            self.update_details_text_initial()

        self.calculate_and_display_rollover()
        self.update_position_display()

    def on_asset_selected(self, event=None):
        selected_asset = self.asset_combo.get()
        if not selected_asset: return
        self.current_asset_price = mt5_get_symbol_price(selected_asset)
        if self.current_asset_price is None: self.clear_all_displays(); return
        bounds = self.current_asset_price * np.array([0.85, 1.15])
        filtered_df = self.df_options[(self.df_options['ativo_principal'] == selected_asset) & (self.df_options['strike'].between(bounds[0], bounds[1]))].copy()
        self.last_filtered_df_for_treeview = filtered_df
        self.update_treeview(filtered_df)
        if not filtered_df.empty: self.sort_treeview_column('expiracao', initial_sort_descending=False); self.highlight_closest_strikes_in_treeview()
        else: self.selected_option_pair = None; self.clear_all_displays()
        if hasattr(self, 'price_entries'): self.price_entries["Ações"]["var"].set(f"{self.current_asset_price:.2f}"); self.price_entries["Calls"]["var"].set(""); self.price_entries["Puts"]["var"].set("")
        self._update_all_dynamic_info()

    def on_tree_select(self, event=None):
        selected_id = self.tree.focus()
        if not selected_id: return
        original_df_index = self.tree_item_map.get(selected_id)
        if original_df_index is None: self.clear_all_displays(); return
        try: self.selected_option_pair = self.last_filtered_df_for_treeview.loc[original_df_index].to_dict()
        except (KeyError, IndexError): self.clear_all_displays(); return
        self.refresh_all_prices()
        self.on_input_change()
        
    def _get_strategy_parameters(self):
        if not self.selected_option_pair: return None
        try:
            return {'asset_p': float(self.price_entries["Ações"]["var"].get().replace(',', '.')), 'asset_q': int(self.qty_spinboxes["Ações"]["var"].get()),
                    'call_p': float(self.price_entries["Calls"]["var"].get().replace(',', '.')), 'call_q': int(self.qty_spinboxes["Calls"]["var"].get()),
                    'put_p': float(self.price_entries["Puts"]["var"].get().replace(',', '.')), 'put_q': int(self.qty_spinboxes["Puts"]["var"].get()),
                    'strike': float(self.selected_option_pair['strike'])}
        except (ValueError, KeyError, TypeError): return None
        
    def _get_unwind_quantities(self):
        try:
            return {
                'asset_q': int(self.unwind_qty_spinboxes["Ações"]["var"].get()),
                'call_q': int(self.unwind_qty_spinboxes["Calls"]["var"].get()),
                'put_q': int(self.unwind_qty_spinboxes["Puts"]["var"].get())
            }
        except (ValueError, KeyError, AttributeError):
            return None

    def _plot_simulation_payout(self):
        """Calcula e plota o payoff para a operação de simulação (gráfico da esquerda)."""
        params = self._get_strategy_parameters()
        if params is None:
            return

        pc_range = np.linspace(-0.30, 0.30, 250)
        expiry_prices = params['asset_p'] * (1 + pc_range)
        pnl_values = np.array([
            ((p - params['asset_p']) * params['asset_q']) +
            ((params['call_p'] - max(0, p - params['strike'])) * params['call_q']) +
            ((max(0, params['strike'] - p) - params['put_p']) * params['put_q'])
            for p in expiry_prices
        ])

        asset_name = self.selected_option_pair.get('ativo_principal') if self.selected_option_pair else None
        self._render_payout_on_axis(self.ax_left, pc_range, pnl_values, params, "Simulação Montagem/Rolagem", asset_name, 'last_graph_pnl_pct_sim')

    def _plot_position_payout(self):
        """Calcula e plota o payoff para a posição atual (gráfico da direita)."""
        if not self.current_position or 'tickers' not in self.current_position:
            return

        params = self.current_position
        if params.get('asset_p', 0) == 0 or params.get('strike') is None:
            return

        pc_range = np.linspace(-0.30, 0.30, 250)
        expiry_prices = params['asset_p'] * (1 + pc_range)
        pnl_values = np.array([
            ((p - params.get('asset_p', 0)) * params.get('asset_q', 0)) +
            ((params.get('call_p', 0) - max(0, p - params.get('strike', 0))) * params.get('call_q', 0)) +
            ((max(0, params.get('strike', 0) - p) - params.get('put_p', 0)) * params.get('put_q', 0))
            for p in expiry_prices
        ])

        asset_name = params.get('tickers', {}).get('asset')
        self._render_payout_on_axis(self.ax_right, pc_range, pnl_values, params, f"Posição Atual ({self.current_position_key})", asset_name, 'last_graph_pnl_pct_pos')

    def _update_payout_graphs(self):
        """Limpa e redesenha ambos os gráficos de payoff."""
        for ax in [self.ax_left, self.ax_right]:
            ax.clear()
            ax.grid(True, linestyle=':', alpha=0.7)
            ax.axhline(0, color='black', linestyle='--', linewidth=1)
            ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=0))

        self.ax_left.set_title("Simulação Montagem/Rolagem", fontsize=9)
        self.ax_right.set_title(f"Posição Atual ({self.current_position_key})", fontsize=9)

        for ax in [self.ax_left, self.ax_right]:
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=1))
            ax.tick_params(axis='y', which='both', left=True, labelleft=True, right=False, labelright=False)
            ax.yaxis.set_ticks_position('left')
            ax.yaxis.set_label_position('left')
        
        self._plot_simulation_payout()
        self._plot_position_payout()
        
        self.fig.tight_layout(pad=0.5)
        self.canvas.draw()

    def calculate_and_plot(self):
        self._update_payout_graphs()
        params = self._get_strategy_parameters()
        if params:
            self._update_summary_widgets(params)
    
    def _calculate_rollover_d2_flow(self, assembly_params, unwind_quantities):
        if not self.current_position or not self.selected_option_pair: return None
        pos, new_pair = self.current_position, self.selected_option_pair
        if pos.get('tickers', {}).get('asset') != new_pair['ativo_principal']: return None
        all_symbols = list(filter(None, set([pos['tickers']['asset'], pos['tickers']['call'], pos['tickers']['put'], new_pair['ticker_call'], new_pair['ticker_put']])))
        prices = mt5_get_all_prices_optimized(all_symbols)
        return self._calculate_rollover_d2_flow_with_prices(assembly_params, unwind_quantities, prices, pos)

    def perform_d2_goal_seek(self):
        try:
            TARGET_D2_VALUE = float(self.goal_seek_target_var.get().replace(',', '.'))
        except (ValueError, TypeError): return
        
        unwind_quantities = self._get_unwind_quantities()
        base_assembly_params = self._get_strategy_parameters()
        
        if not all([unwind_quantities, base_assembly_params, self.current_position, self.selected_option_pair]): return

        q_asset_base = base_assembly_params['asset_q']
        q_call_base = base_assembly_params['call_q']
        q_put_base = base_assembly_params['put_q']
        total_q_base = q_asset_base + q_call_base + q_put_base
        if total_q_base == 0: return
            
        ratio_asset = q_asset_base / total_q_base
        ratio_call = q_call_base / total_q_base
        ratio_put = q_put_base / total_q_base
        multiplier = float(total_q_base) 
        
        for i in range(20): 
            current_params = {'asset_q': multiplier * ratio_asset, 'call_q': multiplier * ratio_call, 'put_q': multiplier * ratio_put}
            current_d2 = self._calculate_rollover_d2_flow(current_params, unwind_quantities)
            if current_d2 is None: return
            error = TARGET_D2_VALUE - current_d2
            if abs(error) < 50: break
            next_params = {'asset_q': (multiplier + 1) * ratio_asset, 'call_q': (multiplier + 1) * ratio_call, 'put_q': (multiplier + 1) * ratio_put}
            next_d2 = self._calculate_rollover_d2_flow(next_params, unwind_quantities)
            if next_d2 is None: return 
            gradient = next_d2 - current_d2
            if abs(gradient) < 1e-9: return
            adjustment = error / gradient
            multiplier += adjustment * 0.8 
            if multiplier < 0: multiplier = 0

        self.qty_spinboxes["Ações"]["var"].set(int(round(multiplier * ratio_asset / 100) * 100))
        self.qty_spinboxes["Calls"]["var"].set(int(round(multiplier * ratio_call / 100) * 100))
        self.qty_spinboxes["Puts"]["var"].set(int(round(multiplier * ratio_put / 100) * 100))
        self.on_input_change()

    def calculate_rollover_for_target_profit(self):
        try:
            target_profit = float(self.target_profit_var.get())
            target_d2_flow = float(self.goal_seek_target_var.get())
        except (ValueError, TypeError):
            messagebox.showerror("Erro", "Valores inválidos para 'Lucro Alvo' ou 'Meta D+2'.")
            return

        if not self.current_position or not self.selected_option_pair:
            messagebox.showwarning("Aviso", "É necessário ter uma posição atual e um par de rolagem selecionado.")
            return

        pos, new_pair = self.current_position, self.selected_option_pair

        unwind_quantities = self._get_unwind_quantities()
        if not unwind_quantities or sum(unwind_quantities.values()) == 0:
            messagebox.showerror("Erro", "As quantidades de desmontagem parcial não podem ser zero.")
            return
        
        pos_qty = {
            'asset': pos.get('asset_q', 0),
            'call': abs(pos.get('call_q', 0)),
            'put': pos.get('put_q', 0)
        }
        if unwind_quantities['asset_q'] > pos_qty['asset'] or \
           unwind_quantities['call_q'] > pos_qty['call'] or \
           unwind_quantities['put_q'] > pos_qty['put']:
            messagebox.showerror("Erro de Quantidade", "A quantidade de desmontagem não pode ser maior que a posição atual.")
            return

        prop_asset = (unwind_quantities['asset_q'] / pos_qty['asset']) if pos_qty['asset'] > 0 else 0
        prop_call = (unwind_quantities['call_q'] / pos_qty['call']) if pos_qty['call'] > 0 else 0
        prop_put = (unwind_quantities['put_q'] / pos_qty['put']) if pos_qty['put'] > 0 else 0

        custo_asset_orig = -(pos['asset_p'] * pos['asset_q'])
        custo_call_orig = (pos['call_p'] * pos['call_q'])
        custo_put_orig = -(pos['put_p'] * pos['put_q'])

        custo_montagem_parcial = (custo_asset_orig * prop_asset) + \
                                 (custo_call_orig * prop_call) + \
                                 (custo_put_orig * prop_put)
        
        target_custo_desmontagem = target_profit - custo_montagem_parcial

        all_symbols_for_prices = list(filter(None, set(list(pos.get('tickers', {}).values()) + [new_pair['ticker_call'], new_pair['ticker_put']])))
        current_market_prices = mt5_get_all_prices_optimized(all_symbols_for_prices)

        asset_bid = current_market_prices.get(f"{pos['tickers']['asset']}_bid", 0)
        call_ask = current_market_prices.get(f"{pos['tickers']['call']}_ask", 0)
        put_bid = current_market_prices.get(f"{pos['tickers']['put']}_bid", 0)
        
        if any(p == 0 for p in [asset_bid, call_ask, put_bid]):
            messagebox.showerror("Erro", "Não foi possível obter preços de mercado para a simulação.")
            return

        current_custo_desmontagem = (asset_bid * unwind_quantities['asset_q']) - \
                                    (call_ask * unwind_quantities['call_q']) + \
                                    (put_bid * unwind_quantities['put_q'])

        denominator = (asset_bid * unwind_quantities['asset_q']) + \
                      (call_ask * unwind_quantities['call_q']) + \
                      (put_bid * unwind_quantities['put_q'])

        if abs(denominator) < 1e-6:
            messagebox.showerror("Erro", "Não é possível calcular a melhora de preço (denominador zero).")
            return
            
        price_improvement_factor = (target_custo_desmontagem - current_custo_desmontagem) / denominator

        future_prices = {}
        for symbol, p_type in [(s, t) for s in all_symbols_for_prices for t in ['ask', 'bid']]:
            key = f"{symbol}_{p_type}"
            current_price = current_market_prices.get(key)
            if current_price:
                future_prices[key] = current_price * (1 + price_improvement_factor if p_type == 'bid' else 1 - price_improvement_factor)
            else:
                 future_prices[key] = 0

        base_assembly_params = self._get_strategy_parameters()
        if not base_assembly_params: return

        q_asset_base, q_call_base, q_put_base = base_assembly_params['asset_q'], base_assembly_params['call_q'], base_assembly_params['put_q']
        total_q_base = q_asset_base + q_call_base + q_put_base
        if total_q_base == 0:
            messagebox.showwarning("Aviso", "As quantidades de montagem base não podem ser zero.")
            return
            
        ratio_asset, ratio_call, ratio_put = q_asset_base / total_q_base, q_call_base / total_q_base, q_put_base / total_q_base
        multiplier = float(total_q_base)
        
        for i in range(20):
            current_params = {'asset_q': multiplier * ratio_asset, 'call_q': multiplier * ratio_call, 'put_q': multiplier * ratio_put}
            current_d2 = self._calculate_rollover_d2_flow_with_prices(current_params, unwind_quantities, future_prices, pos)
            
            if current_d2 is None: messagebox.showerror("Erro", "Falha ao calcular D+2 na simulação."); return
            error = target_d2_flow - current_d2
            if abs(error) < 50: break

            next_params = {'asset_q': (multiplier + 100) * ratio_asset, 'call_q': (multiplier + 100) * ratio_call, 'put_q': (multiplier + 100) * ratio_put}
            next_d2 = self._calculate_rollover_d2_flow_with_prices(next_params, unwind_quantities, future_prices, pos)

            if next_d2 is None: return
            gradient = next_d2 - current_d2
            if abs(gradient) < 1e-9: break
            adjustment = error / gradient
            multiplier += adjustment * 100 * 0.8
            if multiplier < 0: multiplier = 0
            
        final_q_asset = int(round(multiplier * ratio_asset / 100) * 100)
        final_q_call = int(round(multiplier * ratio_call / 100) * 100)
        final_q_put = int(round(multiplier * ratio_put / 100) * 100)

        self.qty_spinboxes["Ações"]["var"].set(final_q_asset)
        self.qty_spinboxes["Calls"]["var"].set(final_q_call)
        self.qty_spinboxes["Puts"]["var"].set(final_q_put)
        
        simulated_assembly_params = self._get_strategy_parameters()
        self._display_rollover_data(new_pair, pos, unwind_quantities, simulated_assembly_params, future_prices)
        self.update_position_display() # Ensure Alvo+Custo is updated
        
    def _calculate_rollover_d2_flow_with_prices(self, assembly_params, unwind_quantities, prices, current_pos):
        pos_call_ask = prices.get(f"{current_pos['tickers']['call']}_ask")
        new_call_bid = prices.get(f"{self.selected_option_pair['ticker_call']}_bid")
        pos_put_bid = prices.get(f"{current_pos['tickers']['put']}_bid")
        new_put_ask = prices.get(f"{self.selected_option_pair['ticker_put']}_ask")
        asset_ask = prices.get(f"{current_pos['tickers']['asset']}_ask")
        asset_bid = prices.get(f"{current_pos['tickers']['asset']}_bid")

        if any(p is None for p in [pos_call_ask, new_call_bid, pos_put_bid, new_put_ask, asset_ask, asset_bid]):
            return None

        fin_recompra_call = -(unwind_quantities['call_q'] * pos_call_ask)
        fin_venda_call = assembly_params['call_q'] * new_call_bid
        fin_venda_put = unwind_quantities['put_q'] * pos_put_bid
        fin_compra_put = -(assembly_params['put_q'] * new_put_ask)
        liquido_opcoes = fin_recompra_call + fin_venda_call + fin_venda_put + fin_compra_put
        
        net_asset_q_change = assembly_params['asset_q'] - unwind_quantities['asset_q']

        fin_asset = 0
        if net_asset_q_change > 0:
            fin_asset = -net_asset_q_change * asset_ask
        elif net_asset_q_change < 0:
            fin_asset = abs(net_asset_q_change) * asset_bid

        return liquido_opcoes + fin_asset

    def calculate_and_display_rollover(self):
        if not self.current_position or not self.selected_option_pair:
            self.update_details_text_initial()
            return

        unwind_quantities = self._get_unwind_quantities()
        assembly_params = self._get_strategy_parameters()
        if not unwind_quantities or not assembly_params:
            self.update_details_text_initial()
            return
            
        pos, new_pair = self.current_position, self.selected_option_pair
        if pos.get('tickers', {}).get('asset') != new_pair['ativo_principal']:
            self.update_details_text_initial()
            return

        all_symbols = list(filter(None, set(list(pos.get('tickers', {}).values()) + [new_pair['ticker_call'], new_pair['ticker_put']])))
        prices = mt5_get_all_prices_optimized(all_symbols)
        self._display_rollover_data(new_pair, pos, unwind_quantities, assembly_params, prices)
    
    def _display_rollover_data(self, new_pair, pos, unwind_quantities, assembly_params, prices):
        # Limpar todos os widgets de rolagem antes de preencher
        self.rolagem_header_label.config(text="")
        self.rolagem_trades_tree.delete(*self.rolagem_trades_tree.get_children())
        self.d1_value_label.config(text="")
        self.d2_value_label.config(text="")
        self.rolagem_footer_label.config(text="")

        pos_call_ask = prices.get(f"{pos['tickers']['call']}_ask", 0)
        pos_put_bid = prices.get(f"{pos['tickers']['put']}_bid", 0)
        new_call_bid = prices.get(f"{new_pair['ticker_call']}_bid", 0)
        new_put_ask = prices.get(f"{new_pair['ticker_put']}_ask", 0)
        asset_ask = prices.get(f"{pos['tickers']['asset']}_ask", 0)
        asset_bid = prices.get(f"{pos['tickers']['asset']}_bid", 0)

        if 0 in [pos_call_ask, pos_put_bid, new_call_bid, new_put_ask, asset_ask, asset_bid]:
             self.rolagem_header_label.config(text="Faltam preços de mercado para simular.")
             return

        # Cálculos financeiros (mesma lógica de antes)
        fin_recompra_call = -(unwind_quantities['call_q'] * pos_call_ask)
        fin_venda_call = assembly_params['call_q'] * new_call_bid
        fin_venda_put = unwind_quantities['put_q'] * pos_put_bid
        fin_compra_put = -(assembly_params['put_q'] * new_put_ask)
        d1_total = fin_recompra_call + fin_venda_call + fin_venda_put + fin_compra_put
        
        net_asset_q_change = assembly_params['asset_q'] - unwind_quantities['asset_q']
        
        fin_asset = 0
        asset_price_used = 0
        if net_asset_q_change > 0:
            asset_price_used = asset_ask
            fin_asset = -net_asset_q_change * asset_price_used
        elif net_asset_q_change < 0:
            asset_price_used = asset_bid
            fin_asset = abs(net_asset_q_change) * asset_price_used
            
        cumulative_d2_flow = d1_total + fin_asset

        # --- Preenchimento dos novos widgets ---

        # 1. Preencher Cabeçalho
        header_text = f"{new_pair['ativo_principal']} | {new_pair['ticker_call']} | {new_pair['ticker_put']}"
        self.rolagem_header_label.config(text=header_text)

        # 2. Preencher Treeview de operações
        def insert_trade_line(label, qty, price, financial):
            tag = "positivo" if financial >= 0 else "negativo"
            # Formata os valores para exibição
            op_text = f"{label}"
            qty_text = f"{qty:,}"
            price_text = f"{price:.2f}"
            financial_text = f"{financial:,.2f}" # Ensure two decimal places
            # Insere a linha na treeview
            self.rolagem_trades_tree.insert('', 'end', values=(op_text, qty_text, price_text, financial_text), tags=(tag,))

        insert_trade_line(f"(C) {pos['tickers']['call']}", unwind_quantities['call_q'], pos_call_ask, fin_recompra_call)
        insert_trade_line(f"(V) {new_pair['ticker_call']}", -assembly_params['call_q'], new_call_bid, fin_venda_call)
        insert_trade_line(f"(V) {pos['tickers']['put']}", -unwind_quantities['put_q'], pos_put_bid, fin_venda_put)
        insert_trade_line(f"(C) {new_pair['ticker_put']}", assembly_params['put_q'], new_put_ask, fin_compra_put)
        
        if net_asset_q_change != 0:
            asset_op_char = 'C' if net_asset_q_change > 0 else 'V'
            insert_trade_line(f"({asset_op_char}) {pos['tickers']['asset']}", net_asset_q_change, asset_price_used, fin_asset)

        # 3. Preencher Labels de D+1 e D+2
        d1_color = "blue" if d1_total >= 0 else "red"
        self.d1_value_label.config(text=f"R$ {d1_total:,.2f}", foreground=d1_color)
        
        d2_color = "blue" if cumulative_d2_flow >= 0 else "red"
        self.d2_value_label.config(text=f"R$ {cumulative_d2_flow:,.2f}", foreground=d2_color)

        # 4. Preencher Rodapé (Alvo+Custo foi movido para update_position_display)
        self.rolagem_footer_label.config(text="") # Limpar o rodapé da rolagem

    def populate_assembly_from_current_position(self):
        if not self.current_position:
            messagebox.showinfo("Aviso", "Nenhuma posição atual carregada para usar.")
            return
        pos = self.current_position
        self.qty_spinboxes["Ações"]["var"].set(pos.get('asset_q', 0))
        self.qty_spinboxes["Calls"]["var"].set(pos.get('call_q', 0))
        self.qty_spinboxes["Puts"]["var"].set(pos.get('put_q', 0))
        self.price_entries["Ações"]["var"].set(f"{pos.get('asset_p', 0):.6f}")
        self.price_entries["Calls"]["var"].set(f"{pos.get('call_p', 0):.6f}")
        self.price_entries["Puts"]["var"].set(f"{pos.get('put_p', 0):.6f}")
        self.trigger_recalculation()

    def update_position_display(self):
        # Clear all parts first
        self.position_tickers_display_label.config(text="")
        # self.position_cal_days_label.config(text="") # Removed
        # self.position_expiry_label.config(text="") # Removed
        self.position_details_tree.delete(*self.position_details_tree.get_children())
        
        summary_widget = self.position_summary_text
        summary_widget.config(state=tk.NORMAL)
        summary_widget.delete(1.0, tk.END)
        summary_widget.config(state=tk.DISABLED)
        
        self.position_alvo_custo_label.config(text="")

        if not self.current_position: 
            self.position_tickers_display_label.config(text="Nenhuma posição montada.")
            self._update_target_profit_pct()
            return
            
        pos = self.current_position
        tickers = pos.get('tickers', {})
        if not tickers:
            self.position_tickers_display_label.config(text="Posição sem tickers definidos.")
            self._update_target_profit_pct()
            return

        # Populate Header Labels
        ticker_str = f"{tickers.get('asset','N/A')} | {tickers.get('call','N/A')} | {tickers.get('put','N/A')}"
        # self.position_tickers_display_label.config(text=ticker_str) # Will be set later with days

        cal_days_str = "N/Ad"
        # exp_date_str = pos.get('expiracao', 'N/A') # No longer displayed separately
        try:
            exp_date_obj = datetime.strptime(pos.get('expiracao', ''), '%d/%m/%Y')
            now = datetime.now()
            cal_days = max(0, (exp_date_obj.date() - now.date()).days)
            cal_days_str = f"{cal_days}d"
        except ValueError: 
            pass 
        
        self.position_tickers_display_label.config(text=f"{ticker_str} | {cal_days_str}")
        # self.position_cal_days_label.config(text=cal_days_str) # Removed
        # self.position_expiry_label.config(text=f"Data Vencimento: {exp_date_str}") # Removed

        # Populate Position Details Tree
        self.position_details_tree.delete(*self.position_details_tree.get_children()) # Clear first
        
        asset_q = pos.get('asset_q', 0)
        asset_p = pos.get('asset_p', 0)
        call_q = pos.get('call_q', 0) # Usually negative for sold calls
        call_p = pos.get('call_p', 0)
        put_q = pos.get('put_q', 0)   # Usually positive for bought puts
        put_p = pos.get('put_p', 0)

        self.position_details_tree.insert('', 'end', values=(tickers.get('asset', 'Ativo N/A'), f"{asset_q:,}", f"{asset_p:.2f}"))
        self.position_details_tree.insert('', 'end', values=(tickers.get('call', 'Call N/A'), f"{call_q:,}", f"{call_p:.2f}"))
        self.position_details_tree.insert('', 'end', values=(tickers.get('put', 'Put N/A'), f"{put_q:,}", f"{put_p:.2f}"))

        prices = mt5_get_all_prices_optimized(list(tickers.values()))
        asset_bid = prices.get(f"{tickers.get('asset')}_bid", 0)
        call_ask = prices.get(f"{tickers.get('call')}_ask", 0)
        put_bid = prices.get(f"{tickers.get('put')}_bid", 0)
        
        custo_montagem = -(asset_p * asset_q) + (call_p * call_q) - (put_p * put_q) # Adjusted to use fetched quantities/prices
        custo_desmontagem = (asset_bid * asset_q) - (call_ask * call_q) + (put_bid * put_q) if all([asset_bid, call_ask, put_bid]) else 0
        resultado_atual = custo_desmontagem + custo_montagem if custo_desmontagem != 0 else 0
        capital_at_risk = pos.get('asset_p', 0) * pos.get('asset_q', 0)
        resultado_pct = (resultado_atual / capital_at_risk) * 100 if capital_at_risk > 0 else 0
        
        graph_pnl_pct = getattr(self, 'last_graph_pnl_pct_pos', 0.0)
        exit_cost_pct = max(0, graph_pnl_pct - resultado_pct)

        # Populate Summary Text Widget
        summary_widget = self.position_summary_text
        summary_widget.config(state=tk.NORMAL)
        summary_widget.delete(1.0, tk.END) # Clear before inserting

        summary_widget.insert(tk.END, "Custos: ")
        summary_widget.insert(tk.END, f"R$ {custo_montagem:,.2f} | ", "positivo" if custo_montagem >= 0 else "negativo")
        summary_widget.insert(tk.END, f"R$ {custo_desmontagem:,.2f}\n", "positivo" if custo_desmontagem >= 0 else "negativo")
        summary_widget.insert(tk.END, "Re. Atual: ")
        summary_widget.insert(tk.END, f"R$ {resultado_atual:,.2f} | ", "positivo" if resultado_atual >= 0 else "negativo")
        summary_widget.insert(tk.END, f"{resultado_pct:+.2f}%\n", "positivo" if resultado_pct >= 0 else "negativo")
        summary_widget.insert(tk.END, "Custo saída: ")
        summary_widget.insert(tk.END, f"{exit_cost_pct:+.2f}%\n", "positivo")
        summary_widget.config(state=tk.DISABLED)

        # Populate Alvo+Custo Label
        try:
            target_profit = float(self.target_profit_var.get())
        except (ValueError, TypeError):
            target_profit = 0
        
        alvo_custo = abs(custo_montagem) + target_profit
        self.position_alvo_custo_label.config(text=f"Alvo+Custo: R$ {alvo_custo:,.2f}", foreground="black")

        self._update_target_profit_pct()

    def _render_payout_on_axis(self, ax, pc_range, pnl_values, params, title, asset_name, pnl_pct_attr_name):
        """Função genérica para renderizar um gráfico de payoff em um eixo (ax) específico."""
        setattr(self, pnl_pct_attr_name, 0.0)
        graph_font_size = 8
        ax.set_title(title, fontsize=9)
        ax.tick_params(axis='both', which='major', labelsize=graph_font_size, colors='blue')

        # Altera a cor da borda (spines) do gráfico para cinza
        for spine in ['top', 'bottom', 'left', 'right']:
            ax.spines[spine].set_edgecolor('gray')

        capital_base = abs((params.get('asset_p', 0) * params.get('asset_q', 0)) -
                           (params.get('call_p', 0) * params.get('call_q', 0)) +
                           (params.get('put_p', 0) * params.get('put_q', 0)))
        if capital_base < 0.01:
            capital_base = params.get('asset_p', 0) * params.get('asset_q', 0)
        
        show_absolute_return = capital_base <= 0.01
        y_axis_values = pnl_values if show_absolute_return else pnl_values / capital_base
        
        ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('R$ %.0f') if show_absolute_return else mtick.PercentFormatter(xmax=1.0, decimals=1))
        
        ax.set_xlim(-0.30, 0.30)
        ax.xaxis.set_major_locator(mtick.MultipleLocator(0.05))
        ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=0))


        line, = ax.plot(pc_range, y_axis_values, linewidth=1.5)
        ax.axvline(0, color='gray', ls='-', lw=0.9)
        
        if params.get('asset_p', 0) != 0 and params.get('strike') is not None:
             ax.axvline((params['strike'] - params['asset_p']) / params['asset_p'], color='red', ls='-', lw=0.9)

        ax.axvline(x=0.12, color='green', ls='-', lw=0.9, alpha=0.7)
        ax.axvline(x=-0.12, color='green', ls='-', lw=0.9, alpha=0.7)

        if asset_name:
            prices = mt5_get_all_prices_optimized([asset_name])
            live_price = prices.get(f'{asset_name}_ask')
            if live_price and params.get('asset_p', 0) != 0 and params.get('strike') is not None:
                x_pos = (live_price - params['asset_p']) / params['asset_p']
                if -0.30 <= x_pos <= 0.30:
                    ax.axvline(x=x_pos, color='green', ls='-', lw=0.9)
                
                pnl_at_live_price = ((live_price - params.get('asset_p', 0)) * params.get('asset_q', 0)) + \
                                    ((params.get('call_p', 0) - max(0, live_price - params.get('strike', 0))) * params.get('call_q', 0)) + \
                                    ((max(0, params.get('strike', 0) - live_price) - params.get('put_p', 0)) * params.get('put_q', 0))
                
                y_at_live_price = pnl_at_live_price if show_absolute_return else (pnl_at_live_price / capital_base if capital_base > 0 else 0)
                
                if -0.30 <= x_pos <= 0.30:
                    ax.plot(x_pos, y_at_live_price, 'o', ms=5, color='green')
                    
                    price_str = f"{live_price:.2f}"
                    financial_str = f"{pnl_at_live_price:,.0f}".replace(",", ".")
                    if show_absolute_return:
                        label_text = f"{price_str} | {financial_str}"
                    else:
                        pnl_percent_at_live_price = (pnl_at_live_price / capital_base) * 100 if capital_base > 0 else 0
                        setattr(self, pnl_pct_attr_name, pnl_percent_at_live_price)
                        percent_str = f"{pnl_percent_at_live_price:.2f}".replace('.', ',') + '%'
                        label_text = f"{price_str} | {percent_str} | {financial_str}"
                    
                    ax.annotate(label_text,
                                xy=(x_pos, y_at_live_price),
                                xytext=(0.85, 0.15),
                                textcoords='axes fraction',
                                ha='center',
                                va='top',
                                fontsize=graph_font_size,
                                bbox=dict(boxstyle="round,pad=0.3", fc="yellow", ec="black", lw=0.5, alpha=0.7))
        for x_pc_annotation in np.arange(-0.25, 0.252, 0.05):
            idx = (np.abs(pc_range - x_pc_annotation)).argmin()
            y_plot_annotation = y_axis_values[idx]
            pnl_absolute_annotation = pnl_values[idx]

            ax.plot(x_pc_annotation, y_plot_annotation, 'o', ms=4, color=line.get_color(), alpha=0.7)

            financial_str_annotation = f"{pnl_absolute_annotation:,.0f}".replace(",", ".")

            if not show_absolute_return and capital_base > 0:
                y_percent_str_annotation = f"{(pnl_absolute_annotation / capital_base) * 100:.1f}%"
                label_text_annotation = f"{y_percent_str_annotation}\n{financial_str_annotation}"
            else:
                label_text_annotation = f"{financial_str_annotation}"

            ax.annotate(label_text_annotation,
                        (x_pc_annotation, y_plot_annotation),
                        textcoords="offset points",
                        xytext=(0, 7 if pnl_absolute_annotation >=0 else -17), # Ajusta a posição vertical da anotação
                        ha='center',
                        va='bottom' if pnl_absolute_annotation >=0 else 'top',
                        fontsize=graph_font_size -1, # Fonte um pouco menor para não poluir muito
                        multialignment='center',
                        bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", lw=0, alpha=0.0)) # Background transparente (alpha=0.0)

    def _update_summary_widgets(self, params):
        if not self.selected_option_pair or not self.mt5_prices:
            self.update_details_text_initial(); return
        
        pair, prices = self.selected_option_pair, self.mt5_prices
        q_asset, q_call, q_put = params['asset_q'], params['call_q'], params['put_q']
        
        p_asset_ask = prices.get('asset_ask', 0) or 0
        p_call_bid = prices.get('call_bid', 0) or 0
        p_put_ask = prices.get('put_ask', 0) or 0
        p_asset_bid = prices.get('asset_bid', 0) or 0
        p_call_ask = prices.get('call_ask', 0) or 0
        p_put_bid = prices.get('put_bid', 0) or 0

        # Atualizar Labels Superiores
        ticker_text = f"{pair['ativo_principal']} | {pair['ticker_call']} | {pair['ticker_put']}"
        
        cal_days_str = "N/Ad" # Default value
        try:
            exp_date = datetime.strptime(pair.get('expiracao', ''), '%d/%m/%Y')
            now = datetime.now()
            cal_days = max(0, (exp_date.date() - now.date()).days)
            cal_days_str = f"{cal_days}d"
        except ValueError: # Handles cases where pair.get('expiracao', '') is empty or invalid format
            pass # cal_days_str remains "N/Ad"
        
        self.montagem_tickers_label.config(text=f"{ticker_text} | {cal_days_str}")

        # Limpar e Preencher Treeview
        self.montagem_details_tree.delete(*self.montagem_details_tree.get_children())

        total_q = q_asset + q_call + q_put
        pct_asset, pct_call, pct_put = ((q_asset/total_q*100), (q_call/total_q*100), (q_put/total_q*100)) if total_q > 0 else (0,0,0)
        
        pnl_flat_part = (pair['strike'] - p_asset_ask + p_call_bid - p_put_ask)
        capital_base_taxa = abs(p_asset_ask - p_call_bid + p_put_ask) # Usar abs para evitar divisão por zero ou negativo
        taxa = (pnl_flat_part / capital_base_taxa) * 100 if capital_base_taxa > 0 else 0
        
        S0, K, Pc, Pp = params['asset_p'], params['strike'], params['call_p'], params['put_p']
        
        # Break Even calculations
        be1_val, be2_val = float('nan'), float('nan')
        if (q_asset - q_call) != 0:
            be1_val = (S0*q_asset - Pc*q_call - K*q_call + Pp*q_put) / (q_asset - q_call)
        if (q_asset - q_put) != 0: # Assuming typical strategy structure for second BE
            be2_val = (S0*q_asset - Pc*q_call - K*q_put + Pp*q_put) / (q_asset - q_put) 

        be1_pct = (be1_val / S0 - 1) * 100 if S0 > 0 and not np.isnan(be1_val) else float('nan')
        be2_pct = (be2_val / S0 - 1) * 100 if S0 > 0 and not np.isnan(be2_val) else float('nan')
        be_str = f"{be1_pct:+.1f}%"
        if not np.isnan(be2_pct) and abs(be1_pct - be2_pct) > 0.01 : # Adicionar segundo BE se for diferente
             be_str += f" | {be2_pct:+.1f}%"

        spread_in = -p_asset_ask + p_call_bid - p_put_ask if all([p_asset_ask, p_call_bid, p_put_ask]) else 0
        spread_out = p_asset_bid - p_call_ask + p_put_bid if all([p_asset_bid, p_call_ask, p_put_bid]) else 0
        custo = -(q_asset * p_asset_ask) + (q_call * p_call_bid) - (q_put * p_put_ask) if all([p_asset_ask, p_call_bid, p_put_ask]) else 0

        tree_data = [
            ("Strike:", f"{pair['strike']:.2f}"),
            ("Peso: ", f"{pct_asset:.0f}% | {pct_call:.0f}% | {pct_put:.0f}%"),
            ("Taxa: ", f"{taxa:.2f}%"),
            ("B.Even: ", be_str),
            ("Spread In: ", f"{spread_in:,.2f}", "positivo" if spread_in >= 0 else "negativo"),
            ("Spread Out: ", f"{spread_out:,.2f}", "positivo" if spread_out >= 0 else "negativo"),
            ("Custo $: ", f"R$ {custo:,.2f}", "positivo" if custo >= 0 else "negativo")
        ]

        for item_data in tree_data:
            key, value = item_data[0], item_data[1]
            color_tag_name = None
            if len(item_data) > 2: # Se tem cor especificada para o valor
                 color_tag_name = item_data[2]

            # Insere a chave (Info) sem tag de cor específica (usará default)
            # Insere o valor com a tag de cor, se aplicável
            # Para fazer isso, precisamos inserir colunas individualmente ou usar itemconfigure após insert
            # A forma mais simples é inserir e depois aplicar a tag à célula específica se Treeview suportar
            # Tkinter Treeview não suporta tags por célula diretamente. Tags são por item ou por coluna.
            # Vamos aplicar a tag ao item inteiro, mas a cor só será visível no valor se o estilo da coluna 'key' for neutro.
            # A melhor abordagem aqui é ter colunas separadas para 'key' e 'value' e aplicar tags de cor ao item.
            # O estilo "NoBorder.Treeview" pode ser configurado para que a tag afete apenas a coluna 'value'.
            # No entanto, a forma como as tags funcionam é no item inteiro.
            # A solução mais robusta seria garantir que a coluna 'key' não tenha cor de foreground pelas tags.
            # Por agora, vamos manter a inserção como está, assumindo que o estilo default da treeview para 'key' é preto.
            # Se a tag "positivo" ou "negativo" também afetar a cor da chave, precisaremos de uma solução mais complexa.
            # O pedido é que o TÍTULO (key) seja preto. As tags "positivo"/"negativo" já existem.
            
            iid = self.montagem_details_tree.insert('', 'end', values=(key, value))
            if color_tag_name:
                self.montagem_details_tree.item(iid, tags=(color_tag_name,))

        self.montagem_details_tree.tag_configure("positivo", foreground="blue")
        self.montagem_details_tree.tag_configure("negativo", foreground="red")
        # A coluna 'key' usará a cor de foreground padrão do widget Treeview, que é tipicamente preto.

        # Atualizar Labels Inferiores (D+1, D+2)
        d1_flow = (q_call * p_call_bid) - (q_put * p_put_ask) if all([p_call_bid, p_put_ask]) else 0
        cumulative_d2_flow = d1_flow - (q_asset * p_asset_ask if p_asset_ask > 0 else 0)

        d1_color = "blue" if d1_flow >= 0 else "red"
        self.montagem_d1_value_label.config(text=f"R$ {d1_flow:,.2f}", foreground=d1_color)
        
        d2_color = "blue" if cumulative_d2_flow >= 0 else "red"
        self.montagem_d2_value_label.config(text=f"R$ {cumulative_d2_flow:,.2f}", foreground=d2_color)

    def auto_load_initial_asset(self):
        if self.asset_combo.get(): self.on_asset_selected()

    def refresh_all_prices(self):
        if not self.selected_option_pair: return
        symbols_to_fetch = [self.selected_option_pair['ativo_principal'], self.selected_option_pair['ticker_call'], self.selected_option_pair['ticker_put']]
        prices_raw = mt5_get_all_prices_optimized(symbols_to_fetch)
        self.mt5_prices = { 'asset_ask': prices_raw.get(f"{self.selected_option_pair['ativo_principal']}_ask"), 'asset_bid': prices_raw.get(f"{self.selected_option_pair['ativo_principal']}_bid"),
            'call_ask': prices_raw.get(f"{self.selected_option_pair['ticker_call']}_ask"), 'call_bid': prices_raw.get(f"{self.selected_option_pair['ticker_call']}_bid"),
            'put_ask': prices_raw.get(f"{self.selected_option_pair['ticker_put']}_ask"), 'put_bid': prices_raw.get(f"{self.selected_option_pair['ticker_put']}_bid"), }
        self.update_price_fields()

    def update_price_fields(self):
        self.price_entries["Ações"]["var"].set(f"{self.mt5_prices.get('asset_ask'):.2f}" if self.mt5_prices.get('asset_ask') else "")
        self.price_entries["Calls"]["var"].set(f"{self.mt5_prices.get('call_bid'):.2f}" if self.mt5_prices.get('call_bid') else "")
        self.price_entries["Puts"]["var"].set(f"{self.mt5_prices.get('put_ask'):.2f}" if self.mt5_prices.get('put_ask') else "")

    def update_treeview(self, df_filtered):
        self.tree.delete(*self.tree.get_children()); self.tree_item_map.clear()
        if df_filtered.empty: return
        for index, row in df_filtered.iterrows():
            display_call = row['ticker_call'][4:] if isinstance(row['ticker_call'], str) and len(row['ticker_call']) > 4 else row['ticker_call']
            display_put = row['ticker_put'][4:] if isinstance(row['ticker_put'], str) and len(row['ticker_put']) > 4 else row['ticker_put']
            item_id = self.tree.insert("", tk.END, values=(display_call, display_put, f"{row['strike']:.2f}", row['expiracao'])); self.tree_item_map[item_id] = index

    def sort_treeview_column(self, col, initial_sort_descending=None):
        items = [(self.tree.set(k, col), k) for k in self.tree.get_children('')]
        reverse = not self._tree_sort_reverse if self._tree_sort_column == col else bool(initial_sort_descending)
        try:
            if col == 'strike': items.sort(key=lambda t: float(str(t[0]).replace(",", ".")), reverse=reverse)
            elif col == 'expiracao': items.sort(key=lambda t: datetime.strptime(t[0], '%d/%m/%Y'), reverse=reverse)
            else: items.sort(key=lambda t: str(t[0]).lower(), reverse=reverse)
        except ValueError: items.sort(key=lambda t: str(t[0]).lower(), reverse=reverse)
        for index, (val, k) in enumerate(items): self.tree.move(k, '', index)
        self._tree_sort_column, self._tree_sort_reverse = col, reverse
        for c in self.tree['columns']: text = self.tree.heading(c, 'text').replace(' ▼', '').replace(' ▲', ''); self.tree.heading(c, text=text + (' ▼' if reverse else ' ▲') if c == col else text)
        self.highlight_closest_strikes_in_treeview()

    def highlight_closest_strikes_in_treeview(self):
        if not self.tree.get_children() or self.current_asset_price is None: return
        [self.tree.item(item_id, tags=()) for item_id in self.tree.get_children()]
        items_by_exp = {}
        for item_id in self.tree.get_children(''):
            exp = self.tree.set(item_id, 'expiracao'); items_by_exp.setdefault(exp, []).append({'id': item_id, 'strike': float(self.tree.set(item_id, 'strike').replace(",", "."))})
        for exp, items in items_by_exp.items():
            if not items: continue
            closest_item = min(items, key=lambda x: abs(x['strike'] - self.current_asset_price))
            for item in items:
                if abs(item['strike'] - closest_item['strike']) < 1e-6: self.tree.item(item['id'], tags=('closest_strike',))

    def clear_all_displays(self):
        self.tree.delete(*self.tree.get_children()); self.clear_plots(); self.update_details_text_initial()
        [entry["var"].set("") for entry in self.price_entries.values()]; self.last_filtered_df_for_treeview = pd.DataFrame(); self.mt5_prices = {}; self.tree_item_map.clear()

    def clear_plots(self):
        self.ax_left.clear()
        self.ax_right.clear()
        for ax in [self.ax_left, self.ax_right]:
            ax.grid(True, linestyle=':', alpha=0.7)
            ax.axhline(0, color='black', linestyle='--', linewidth=1)
        self.ax_left.set_title("Simulação Montagem/Rolagem", fontsize=9)
        self.ax_right.set_title(f"Posição Atual ({self.current_position_key})", fontsize=9)
        self.last_graph_pnl_pct_sim = 0.0
        self.last_graph_pnl_pct_pos = 0.0
        try:
            self.fig.tight_layout(pad=0.5)
        except Exception:
            pass
        self.canvas.draw()

    def _update_text_widget(self, widget, content):
        widget.config(state=tk.NORMAL); widget.delete(1.0, tk.END); widget.insert(tk.END, content); widget.config(state=tk.DISABLED)

    def update_details_text_initial(self):
        # Limpa os widgets de rolagem
        if hasattr(self, 'rolagem_header_label'):
            self.rolagem_header_label.config(text="Monte uma posição e selecione um novo par para simular.")
            if hasattr(self, 'rolagem_trades_tree'): # Verificar se existe antes de limpar
                self.rolagem_trades_tree.delete(*self.rolagem_trades_tree.get_children())
            self.d1_value_label.config(text="")
            self.d2_value_label.config(text="")
            self.rolagem_footer_label.config(text="")
        
        # Limpa os novos widgets de montagem
        if hasattr(self, 'montagem_tickers_label'):
            self.montagem_tickers_label.config(text="Selecione um par de opções...")
            # self.montagem_vencimento_label.config(text="") # Removed this line
            if hasattr(self, 'montagem_details_tree'): # Verificar se existe antes de limpar
                 self.montagem_details_tree.delete(*self.montagem_details_tree.get_children())
            self.montagem_d1_value_label.config(text="")
            self.montagem_d2_value_label.config(text="")
            # Adicionar uma mensagem inicial na treeview de montagem, se estiver vazia
            if hasattr(self, 'montagem_details_tree') and not self.montagem_details_tree.get_children():
                self.montagem_details_tree.insert('', 'end', values=(">", "Preencha os dados da operação para simular."))


    def show_rollover_context_menu(self, event):
        try:
            self.rollover_context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.rollover_context_menu.grab_release()

    def copy_rollover_basket_to_clipboard(self):
        if not self.current_position or not self.selected_option_pair:
            messagebox.showwarning("Aviso", "Não há dados de rolagem para copiar.\nMonte uma posição e selecione um novo par.")
            return

        unwind_quantities = self._get_unwind_quantities()
        assembly_params = self._get_strategy_parameters()

        if not unwind_quantities or not assembly_params:
            messagebox.showwarning("Aviso", "Verifique as quantidades de montagem e desmontagem.")
            return

        pos, new_pair = self.current_position, self.selected_option_pair
        if pos.get('tickers', {}).get('asset') != new_pair['ativo_principal']:
            messagebox.showwarning("Aviso", "A rolagem deve ser para o mesmo ativo-objeto.")
            return

        basket_lines = []
        tickers = pos.get('tickers', {})
        if (q := unwind_quantities.get('call_q', 0)) > 0: basket_lines.append(f"{tickers.get('call')}\tC\t{q}")
        if (q := assembly_params.get('call_q', 0)) > 0: basket_lines.append(f"{new_pair['ticker_call']}\tV\t{q}")
        if (q := unwind_quantities.get('put_q', 0)) > 0: basket_lines.append(f"{tickers.get('put')}\tV\t{q}")
        if (q := assembly_params.get('put_q', 0)) > 0: basket_lines.append(f"{new_pair['ticker_put']}\tC\t{q}")
            
        net_asset_q_change = assembly_params.get('asset_q', 0) - unwind_quantities.get('asset_q', 0)
        if net_asset_q_change > 0: basket_lines.append(f"{tickers.get('asset')}\tC\t{int(net_asset_q_change)}")
        elif net_asset_q_change < 0: basket_lines.append(f"{tickers.get('asset')}\tV\t{int(abs(net_asset_q_change))}")
            
        if not basket_lines:
            messagebox.showinfo("Aviso", "Nenhuma operação com quantidade maior que zero para copiar.")
            return
            
        clipboard_content = "\n".join(basket_lines)
        try:
            self.root.clipboard_clear()
            self.root.clipboard_append(clipboard_content)
            messagebox.showinfo("Copiado", f"Basket de rolagem copiado para a área de transferência.\n\n{clipboard_content}")
        except tk.TclError:
            messagebox.showerror("Erro", "Não foi possível acessar a área de transferência.")

    def run_sync_scripts_threaded(self, progress_popup_instance):
        if hasattr(self, 'sync_btn'):
            self.root.after_idle(lambda: self.sync_btn.config(state=tk.DISABLED, text="Sincronizando..."))

        try:
            self.root.after_idle(progress_popup_instance.update_progress, "notas", "Executando...")
            
            child_env = os.environ.copy()
            child_env["PYTHONIOENCODING"] = "utf-8"
            
            process_notas = subprocess.run([sys.executable, 'notas.py'], capture_output=True, text=True, check=False, encoding='utf-8', errors='replace', env=child_env)

            if process_notas.returncode == 0:
                self.root.after_idle(progress_popup_instance.update_progress, "notas", "Concluído!", 100)
                self.root.after_idle(progress_popup_instance.update_progress, "relat", "Executando...")
                
                process_relat = subprocess.run([sys.executable, 'relat.py'], capture_output=True, text=True, check=False, encoding='utf-8', errors='replace', env=child_env)

                if process_relat.returncode == 0:
                    self.root.after_idle(progress_popup_instance.update_progress, "relat", "Concluído!", 100)
                    self.root.after_idle(progress_popup_instance.show_close_button)
                else:
                    self.root.after_idle(progress_popup_instance.update_progress, "relat", "Erro!", 0)
                    error_message = f"Erro ao executar relat.py:\nReturn Code: {process_relat.returncode}\nOutput:\n{process_relat.stdout}\nError:\n{process_relat.stderr}"
                    print(error_message)
                    parent_window = progress_popup_instance.popup if progress_popup_instance and hasattr(progress_popup_instance, 'popup') and progress_popup_instance.popup.winfo_exists() else self.root
                    self.root.after_idle(messagebox.showerror, "Erro em relat.py", error_message, parent=parent_window)
                    self.root.after_idle(progress_popup_instance.show_close_button)
            else:
                self.root.after_idle(progress_popup_instance.update_progress, "notas", "Erro!", 0)
                self.root.after_idle(progress_popup_instance.update_progress, "relat", "Não executado", 0)
                error_message = f"Erro ao executar notas.py:\nReturn Code: {process_notas.returncode}\nOutput:\n{process_notas.stdout}\nError:\n{process_notas.stderr}"
                print(error_message)
                parent_window = progress_popup_instance.popup if progress_popup_instance and hasattr(progress_popup_instance, 'popup') and progress_popup_instance.popup.winfo_exists() else self.root
                self.root.after_idle(messagebox.showerror, "Erro em notas.py", error_message, parent=parent_window)
                self.root.after_idle(progress_popup_instance.show_close_button)

        except FileNotFoundError as e:
            self.root.after_idle(progress_popup_instance.update_progress, "notas", "Erro de Arquivo!", 0)
            self.root.after_idle(progress_popup_instance.update_progress, "relat", "Erro de Arquivo!", 0)
            error_msg_fnf = f"Script não encontrado: {e}. Certifique-se que notas.py e relat.py estão no diretório raiz."
            print(error_msg_fnf)
            parent_window = progress_popup_instance.popup if progress_popup_instance and hasattr(progress_popup_instance, 'popup') and progress_popup_instance.popup.winfo_exists() else self.root
            self.root.after_idle(messagebox.showerror, "Erro de Arquivo", error_msg_fnf, parent=parent_window)
            self.root.after_idle(progress_popup_instance.show_close_button)
        except Exception as e:
            self.root.after_idle(progress_popup_instance.update_progress, "notas", "Erro Inesperado!", 0)
            self.root.after_idle(progress_popup_instance.update_progress, "relat", "Erro Inesperado!", 0)
            error_msg_exc = f"Ocorreu um erro inesperado: {e}"
            print(error_msg_exc)
            parent_window = progress_popup_instance.popup if progress_popup_instance and hasattr(progress_popup_instance, 'popup') and progress_popup_instance.popup.winfo_exists() else self.root
            self.root.after_idle(messagebox.showerror, "Erro Inesperado", error_msg_exc, parent=parent_window)
            self.root.after_idle(progress_popup_instance.show_close_button)
        finally:
            if hasattr(self, 'sync_btn') and self.sync_btn.winfo_exists():
                 self.root.after_idle(lambda: self.sync_btn.config(state=tk.NORMAL, text="Sy"))

    def run_sync_scripts(self):
        self.progress_popup = SyncProgressPopup(self.root) 
        thread = threading.Thread(target=self.run_sync_scripts_threaded, args=(self.progress_popup,))
        thread.daemon = True
        thread.start()

    def run_si_extraction(self):
        self.si_progress_popup = SIProgressPopup(self.root)
        thread = threading.Thread(target=self.run_si_extraction_threaded, args=(self.si_progress_popup,))
        thread.daemon = True
        thread.start()

    def run_si_extraction_threaded(self, progress_popup_instance):
        if hasattr(self, 'si_btn'):
            self.root.after_idle(lambda: self.si_btn.config(state=tk.DISABLED, text="SI..."))

        zip_filename = "SI_D_SEDE.zip"
        extract_folder = "SI_D_SEDE"
        file_to_extract = "SI_D_SEDE.txt"
        full_file_path = os.path.join(extract_folder, file_to_extract)
        
        parent_window = progress_popup_instance.popup if progress_popup_instance and hasattr(progress_popup_instance, 'popup') and progress_popup_instance.popup.winfo_exists() else self.root
        
        child_env = os.environ.copy()
        child_env["PYTHONIOENCODING"] = "utf-8"

        try:
            self.root.after_idle(progress_popup_instance.update_progress, "download", "Executando...")
            process_down = subprocess.run([sys.executable, 'down.py'], capture_output=True, text=True, check=False, encoding='utf-8', errors='replace', env=child_env)

            if process_down.returncode == 0:
                self.root.after_idle(progress_popup_instance.update_progress, "download", "Concluído!", 100)
            else:
                self.root.after_idle(progress_popup_instance.update_progress, "download", "Erro!", 0)
                self.root.after_idle(progress_popup_instance.update_progress, "si", "Não executado", 0)
                self.root.after_idle(progress_popup_instance.update_progress, "sync", "Não executado", 0)
                error_message = f"Erro ao executar down.py:\nReturn Code: {process_down.returncode}\nOutput:\n{process_down.stdout}\nError:\n{process_down.stderr}"
                self.root.after_idle(lambda: messagebox.showerror("Erro em down.py", error_message, parent=parent_window))
                self.root.after_idle(progress_popup_instance.show_close_button)
                return

            self.root.after_idle(progress_popup_instance.update_progress, "si", "Iniciando...", 0)
            if not os.path.exists(extract_folder):
                os.makedirs(extract_folder)
                self.root.after_idle(progress_popup_instance.update_progress, "si", f"Pasta {extract_folder} criada.", 20)
            else:
                self.root.after_idle(progress_popup_instance.update_progress, "si", f"Pasta {extract_folder} existente.", 20)

            if not os.path.exists(zip_filename):
                error_message = f"Arquivo {zip_filename} não encontrado. O download pode ter falhado."
                self.root.after_idle(progress_popup_instance.update_progress, "si", "Erro de Arquivo!", 0)
                self.root.after_idle(messagebox.showerror, "Erro de Arquivo", error_message, parent=parent_window)
                self.root.after_idle(progress_popup_instance.show_close_button)
                return

            self.root.after_idle(progress_popup_instance.update_progress, "si", f"Descompactando {file_to_extract}...", 40)
            
            with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                if os.path.exists(full_file_path):
                    os.remove(full_file_path)
                if file_to_extract not in zip_ref.namelist():
                    error_message = f"Arquivo {file_to_extract} não encontrado dentro de {zip_filename}."
                    self.root.after_idle(progress_popup_instance.update_progress, "si", "Erro no ZIP!", 0)
                    self.root.after_idle(messagebox.showerror, "Erro no ZIP", error_message, parent=parent_window)
                    self.root.after_idle(progress_popup_instance.show_close_button)
                    return
                zip_ref.extract(file_to_extract, extract_folder)
            
            self.root.after_idle(progress_popup_instance.update_progress, "si", "Concluído!", 100)
            
            self.root.after_idle(progress_popup_instance.update_progress, "sync", "Executando...")
            process_sync = subprocess.run([sys.executable, 'sync.py'], capture_output=True, text=True, check=False, encoding='utf-8', errors='replace', env=child_env)
            if process_sync.returncode == 0:
                self.root.after_idle(progress_popup_instance.update_progress, "sync", "Concluído!", 100)
                self.root.after_idle(self.load_data)
                self.root.after_idle(self.on_asset_selected)
            else:
                self.root.after_idle(progress_popup_instance.update_progress, "sync", "Erro!", 0)
                error_message_sync = f"Erro ao executar sync.py:\nReturn Code: {process_sync.returncode}\nOutput:\n{process_sync.stdout}\nError:\n{process_sync.stderr}"
                self.root.after_idle(lambda: messagebox.showerror("Erro em sync.py", error_message_sync, parent=parent_window))
            
            self.root.after_idle(progress_popup_instance.show_close_button)

        except FileNotFoundError as e:
            script_name = e.filename
            self.root.after_idle(progress_popup_instance.update_progress, "download" if script_name == "down.py" else "sync", "Erro de Arquivo!", 0)
            error_message = f"Script não encontrado: {script_name}. Certifique-se que o arquivo está no diretório raiz."
            self.root.after_idle(messagebox.showerror, "Erro de Arquivo", error_message, parent=parent_window)
            self.root.after_idle(progress_popup_instance.show_close_button)
        except zipfile.BadZipFile:
            self.root.after_idle(progress_popup_instance.update_progress, "si", "Erro de ZIP!", 0)
            self.root.after_idle(progress_popup_instance.update_progress, "sync", "Não executado", 0)
            error_message = f"Arquivo '{zip_filename}' não é um arquivo ZIP válido ou está corrompido."
            self.root.after_idle(messagebox.showerror, "Erro de ZIP", error_message, parent=parent_window)
            self.root.after_idle(progress_popup_instance.show_close_button)
        except Exception as e:
            self.root.after_idle(progress_popup_instance.update_progress, "download", "Erro Inesperado!", 0)
            self.root.after_idle(progress_popup_instance.update_progress, "si", "Não executado", 0)
            self.root.after_idle(progress_popup_instance.update_progress, "sync", "Não executado", 0)
            error_msg_exc = f"Ocorreu um erro inesperado no processo SI: {e}"
            self.root.after_idle(lambda: messagebox.showerror("Erro Inesperado", error_msg_exc, parent=parent_window))
            self.root.after_idle(progress_popup_instance.show_close_button)
        finally:
            if hasattr(self, 'si_btn') and self.si_btn.winfo_exists():
                self.root.after_idle(lambda: self.si_btn.config(state=tk.NORMAL, text="SI"))

    def show_fiscal_report_popup(self, file_path, title):
        """Abre um popup para exibir dados fiscais de um arquivo JSON."""
        if not os.path.exists(file_path):
            messagebox.showerror("Arquivo não encontrado", f"O arquivo fiscal '{file_path}' não foi encontrado.\nVerifique se o caminho está correto.", parent=self.root)
            return

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError:
            messagebox.showerror("Erro de Leitura", f"O arquivo '{file_path}' não é um JSON válido.", parent=self.root)
            return
        except Exception as e:
            messagebox.showerror("Erro", f"Ocorreu um erro ao ler o arquivo: {e}", parent=self.root)
            return

        popup = tk.Toplevel(self.root)
        popup.title(title)
        popup.transient(self.root)
        popup.grab_set()
        popup.geometry("950x550")
        popup.minsize(700, 400)

        main_frame = ttk.Frame(popup, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        main_frame.rowconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=1)
        main_frame.columnconfigure(0, weight=1)

        pos_data = data.get("posicao_atual_carteira", [])
        res_data = data.get("resultado_mensal_com_compensacao_fiscal", [])

        self._create_fiscal_table(main_frame, "Posição Atual Carteira", pos_data).grid(row=0, column=0, sticky="nsew", pady=(0, 5))
        self._create_fiscal_table(main_frame, "Resultado Mensal com Compensação Fiscal", res_data).grid(row=1, column=0, sticky="nsew", pady=(5, 5))

        close_btn = ttk.Button(main_frame, text="Fechar", command=popup.destroy)
        close_btn.grid(row=2, column=0, pady=(10, 0))

        popup.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() / 2) - (popup.winfo_width() / 2)
        y = self.root.winfo_y() + (self.root.winfo_height() / 2) - (popup.winfo_height() / 2)
        popup.geometry(f"+{int(x)}+{int(y)}")

        popup.wait_window()

    def _create_fiscal_table(self, parent, title, data_list):
        """Helper para criar uma tabela (Treeview) estilizada dentro do popup."""
        frame = ttk.LabelFrame(parent, text=title)
        
        if not data_list or not isinstance(data_list, list) or not isinstance(data_list[0], dict):
            ttk.Label(frame, text="Nenhum dado disponível ou formato inválido.").pack(pady=10, padx=10)
            return frame

        cols = list(data_list[0].keys())
        tree_frame = ttk.Frame(frame)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        tree = ttk.Treeview(tree_frame, columns=cols, show='headings')
        
        tree.tag_configure('oddrow', background='white')
        tree.tag_configure('evenrow', background='#F0F0F8')

        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        hsb.pack(side=tk.BOTTOM, fill=tk.X, padx=5, pady=(0,5))
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        for col in cols:
            tree.heading(col, text=col)
            anchor = tk.W
            tree.column(col, width=100, anchor=anchor, stretch=tk.YES)

        for i, item in enumerate(data_list):
            tag = 'evenrow' if i % 2 == 0 else 'oddrow'
            values = [item.get(c, "") for c in cols]
            tree.insert("", "end", values=values, tags=(tag,))

        return frame

    def _force_taskbar_icon(self):
        """Força a aparição do ícone na barra de tarefas"""
        try:
            # Força foco e atualização
            if hasattr(self, 'root') and self.root.winfo_exists(): # Garante que a janela ainda existe
                self.root.lift()
                self.root.focus_force()
                self.root.update()
            
            # Tenta reconfigurar o ícone
            if hasattr(self, 'root') and self.root.winfo_exists():
                try:
                    self.root.iconbitmap('icon.ico')
                except tk.TclError: # Captura TclError especificamente para iconbitmap
                    pass
        except Exception as e:
            # Evita erros se self.root não existir mais (ex: durante o fechamento)
            if hasattr(self, 'root') and self.root.winfo_exists():
                 print(f"Erro ao forçar ícone: {e}")
            # else: ignore, a janela provavelmente está sendo destruída

class SyncProgressPopup:
    def __init__(self, master):
        self.master = master
        self.popup = tk.Toplevel(master)
        self.popup.title("Sincronizando Scripts...")
        self.popup.transient(master)
        self.popup.grab_set()
        self.popup.geometry("450x150")
        self.popup.protocol("WM_DELETE_WINDOW", lambda: None)


        pad_options = {'padx': 10, 'pady': 5}

        ttk.Label(self.popup, text="notas.py:").grid(row=0, column=0, sticky=tk.W, **pad_options)
        self.notas_progress = ttk.Progressbar(self.popup, orient=tk.HORIZONTAL, length=150, mode='indeterminate')
        self.notas_progress.grid(row=0, column=1, **pad_options)
        self.notas_status_label = ttk.Label(self.popup, text="Aguardando...")
        self.notas_status_label.grid(row=0, column=2, sticky=tk.W, **pad_options)

        ttk.Label(self.popup, text="relat.py:").grid(row=1, column=0, sticky=tk.W, **pad_options)
        self.relat_progress = ttk.Progressbar(self.popup, orient=tk.HORIZONTAL, length=150, mode='indeterminate')
        self.relat_progress.grid(row=1, column=1, **pad_options)
        self.relat_status_label = ttk.Label(self.popup, text="Aguardando...")
        self.relat_status_label.grid(row=1, column=2, sticky=tk.W, **pad_options)

        self.popup.update_idletasks()
        x = master.winfo_x() + (master.winfo_width() / 2) - (self.popup.winfo_width() / 2)
        y = master.winfo_y() + (master.winfo_height() / 2) - (self.popup.winfo_height() / 2)
        self.popup.geometry(f"+{int(x)}+{int(y)}")
        
        self.notas_progress.start()
        self.relat_progress.start()

    def update_progress(self, script_name, status, progress_value=None):
        if script_name == "notas":
            progress_bar = self.notas_progress
            status_label = self.notas_status_label
        elif script_name == "relat":
            progress_bar = self.relat_progress
            status_label = self.relat_status_label
        else:
            return

        status_label.config(text=status)

        if progress_value is None:
            progress_bar.config(mode='indeterminate')
            progress_bar.start()
        else:
            progress_bar.stop()
            progress_bar.config(mode='determinate', value=progress_value)
            if progress_value == 100:
                 progress_bar['value'] = 100


    def show_close_button(self):
        self.popup.protocol("WM_DELETE_WINDOW", self.close)
        
        self.close_button = ttk.Button(self.popup, text="Fechar", command=self.close)
        self.close_button.grid(row=2, column=0, columnspan=3, pady=10)

    def close(self):
        self.popup.grab_release()
        self.popup.destroy()

class SIProgressPopup:
    def __init__(self, master):
        self.master = master
        self.popup = tk.Toplevel(master)
        self.popup.title("Processando SI...")
        self.popup.transient(master)
        self.popup.grab_set()
        self.popup.geometry("500x210")
        self.popup.protocol("WM_DELETE_WINDOW", lambda: None)

        pad_options = {'padx': 10, 'pady': 5}

        ttk.Label(self.popup, text="Download SI:").grid(row=0, column=0, sticky=tk.W, **pad_options)
        self.download_progress = ttk.Progressbar(self.popup, orient=tk.HORIZONTAL, length=150, mode='indeterminate')
        self.download_progress.grid(row=0, column=1, **pad_options)
        self.download_status_label = ttk.Label(self.popup, text="Aguardando...")
        self.download_status_label.grid(row=0, column=2, sticky=tk.W, **pad_options)

        ttk.Label(self.popup, text="Extração ZIP:").grid(row=1, column=0, sticky=tk.W, **pad_options)
        self.si_progress = ttk.Progressbar(self.popup, orient=tk.HORIZONTAL, length=150, mode='indeterminate')
        self.si_progress.grid(row=1, column=1, **pad_options)
        self.si_status_label = ttk.Label(self.popup, text="Aguardando...")
        self.si_status_label.grid(row=1, column=2, sticky=tk.W, **pad_options)

        ttk.Label(self.popup, text="Sincronização:").grid(row=2, column=0, sticky=tk.W, **pad_options)
        self.sync_progress = ttk.Progressbar(self.popup, orient=tk.HORIZONTAL, length=150, mode='indeterminate')
        self.sync_progress.grid(row=2, column=1, **pad_options)
        self.sync_status_label = ttk.Label(self.popup, text="Aguardando...")
        self.sync_status_label.grid(row=2, column=2, sticky=tk.W, **pad_options)

        self.popup.update_idletasks()
        x = master.winfo_x() + (master.winfo_width() / 2) - (self.popup.winfo_width() / 2)
        y = master.winfo_y() + (master.winfo_height() / 2) - (self.popup.winfo_height() / 2)
        self.popup.geometry(f"+{int(x)}+{int(y)}")
        
        self.download_progress.start()
        self.si_progress.start()
        self.sync_progress.start()

    def update_progress(self, task_name, status, progress_value=None):
        progress_bar = None
        status_label = None

        if task_name == "download":
            progress_bar = self.download_progress
            status_label = self.download_status_label
        elif task_name == "si": 
            progress_bar = self.si_progress
            status_label = self.si_status_label
        elif task_name == "sync":
            progress_bar = self.sync_progress
            status_label = self.sync_status_label
        else:
            return

        if status_label and progress_bar:
            status_label.config(text=status)
            if progress_value is None:
                progress_bar.config(mode='indeterminate')
                progress_bar.start()
            else:
                progress_bar.stop() 
                progress_bar.config(mode='determinate', value=progress_value)
                if progress_value == 100: 
                    progress_bar['value'] = 100
                elif progress_value == 0 and status.startswith("Erro"): 
                    progress_bar['value'] = 0

    def show_close_button(self):
        self._create_close_button()

    def _create_close_button(self):
        if self.popup.winfo_exists():
            self.popup.protocol("WM_DELETE_WINDOW", self.close)
            if not hasattr(self, 'close_button') or not self.close_button.winfo_exists():
                self.close_button = ttk.Button(self.popup, text="Fechar", command=self.close)
                self.close_button.grid(row=3, column=0, columnspan=3, pady=10) # Row ajustada

    def close(self):
        if self.popup.winfo_exists():
            self.popup.grab_release()
            self.popup.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    app = OptionStrategyApp(root)
    if app.df_options is not None and not app.df_options.empty:
        root.mainloop()