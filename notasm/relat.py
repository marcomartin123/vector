import pandas as pd
from datetime import datetime
from collections import defaultdict
import re
import csv
import json  # Adicionado para salvar o resultado em JSON

# ======================================
# CONFIGURAÇÃO FISCAL
# ======================================
# Prejuízo acumulado trazido de anos/meses anteriores
# Altere este valor para incluir prejuízos fiscais anteriores
PREJUIZO_ACUMULADO_ANTERIOR = -50000.00  # Exemplo: -50.000,00 de prejuízo anterior
# ======================================

try:
    from tabulate import tabulate
    TABULATE_AVAILABLE = True
except ImportError:
    TABULATE_AVAILABLE = False
    print("⚠️  Biblioteca 'tabulate' não encontrada. Instale com: pip install tabulate")
    print("   Usando formatação simples...")

def convert_brazilian_number(value_str, is_integer=False):
    """Convert Brazilian number format to Python number"""
    if is_integer:
        # For quantities: remove dots (thousands separator)
        return int(value_str.replace('.', ''))
    else:
        # For prices and values: remove dots (thousands) and replace comma with dot (decimal)
        # Handle cases like "2.275,00" or "0,91"
        if ',' in value_str:
            # Split by comma to get decimal part
            integer_part, decimal_part = value_str.split(',')
            # Remove dots from integer part
            integer_part = integer_part.replace('.', '')
            return float(f"{integer_part}.{decimal_part}")
        else:
            # No decimal part, just remove dots
            return float(value_str.replace('.', ''))

def parse_trading_data(file_content):
    """
    Parse the trading data from the extracted notes format
    """
    operations = []
    
    # Split by notes
    notes = file_content.split('--- Nota:')
    
    for note in notes[1:]:  # Skip the first empty split
        lines = note.strip().split('\n')
        
        # Extract note info
        note_info = lines[0]
        
        # Find transaction lines (start with C| or D|)
        for line in lines:
            if line.startswith(('C|', 'D|')):
                parts = line.split('|')
                if len(parts) >= 7:
                    try:
                        operation_type = parts[0]  # C or D
                        ticker = parts[1]
                        date = parts[2]
                        quantity = convert_brazilian_number(parts[3], is_integer=True)
                        price = convert_brazilian_number(parts[4])
                        gross_value = convert_brazilian_number(parts[5])
                        net_value = convert_brazilian_number(parts[6])
                        
                        operations.append({
                            'type': operation_type,
                            'ticker': ticker,
                            'date': datetime.strptime(date, '%d/%m/%Y'),
                            'quantity': quantity,
                            'price': price,
                            'gross_value': gross_value,
                            'net_value': net_value,
                            'month': date[3:10]  # MM/YYYY format
                        })
                    except (ValueError, IndexError) as e:
                        print(f"⚠️  Erro ao processar linha: {line}")
                        print(f"   Erro: {e}")
                        continue
    
    return operations

def generate_fifo_extract(operations):
    """
    Generate detailed FIFO extract showing all position openings and closures
    """
    grouped_ops = group_by_asset(operations)
    fifo_extract = []
    
    print("📋 Processando ativos para extrato FIFO...")
    
    for asset, ops in grouped_ops.items():
        print(f"   Processando {asset}: {len(ops)} operações")
        
        # Sort operations by date for FIFO calculation
        ops.sort(key=lambda x: x['date'])
        
        # Simple FIFO tracking
        open_positions = []  # [{'qty': int, 'price': float, 'date': str, 'is_long': bool}, ...]
        
        for op in ops:
            op_date = op['date'].strftime('%d/%m/%Y')
            price_per_share = op['net_value'] / op['quantity']
            
            if op['type'] == 'D':  # Compra
                # First close any short positions
                remaining_qty = op['quantity']
                
                while remaining_qty > 0 and open_positions and not open_positions[0]['is_long']:
                    pos = open_positions[0]
                    close_qty = min(remaining_qty, pos['qty'])
                    
                    # Calculate P&L for short closure
                    pnl = close_qty * (pos['price'] - price_per_share)
                    days_held = (op['date'] - datetime.strptime(pos['date'], '%d/%m/%Y')).days
                    
                    fifo_extract.append({
                        'Asset': asset,
                        'Ticker': op['ticker'],
                        'Tipo_Operacao': 'FECHAMENTO_SHORT',
                        'Data_Abertura': pos['date'],
                        'Data_Fechamento': op_date,
                        'Quantidade_Abertura': pos['qty'],
                        'Preco_Abertura': f"{pos['price']:.2f}".replace('.', ','),
                        'Quantidade_Fechamento': close_qty,
                        'Preco_Fechamento': f"{price_per_share:.2f}".replace('.', ','),
                        'Tipo_Fechamento': 'TOTAL' if close_qty == pos['qty'] else 'PARCIAL',
                        'PL_Realizado': f"{pnl:.2f}".replace('.', ','),
                        'Valor_Abertura': f"{pos['qty'] * pos['price']:.2f}".replace('.', ','),
                        'Valor_Fechamento': f"{close_qty * price_per_share:.2f}".replace('.', ','),
                        'Dias_Posicao': str(days_held),
                        'Retorno_Percent': f"{(pnl / (close_qty * pos['price'])) * 100:.2f}".replace('.', ',') + '%' if pos['price'] > 0 else '0,00%'
                    })
                    
                    # Update position
                    if close_qty == pos['qty']:
                        open_positions.pop(0)
                    else:
                        open_positions[0]['qty'] -= close_qty
                    
                    remaining_qty -= close_qty
                
                # Add remaining as long position
                if remaining_qty > 0:
                    open_positions.append({
                        'qty': remaining_qty,
                        'price': price_per_share,
                        'date': op_date,
                        'is_long': True
                    })
                    
                    fifo_extract.append({
                        'Asset': asset,
                        'Ticker': op['ticker'],
                        'Tipo_Operacao': 'ABERTURA_LONG',
                        'Data_Abertura': op_date,
                        'Data_Fechamento': '',
                        'Quantidade_Abertura': remaining_qty,
                        'Preco_Abertura': f"{price_per_share:.2f}".replace('.', ','),
                        'Quantidade_Fechamento': 0,
                        'Preco_Fechamento': '',
                        'Tipo_Fechamento': '',
                        'PL_Realizado': '0,00',
                        'Valor_Abertura': f"{remaining_qty * price_per_share:.2f}".replace('.', ','),
                        'Valor_Fechamento': '',
                        'Dias_Posicao': '',
                        'Retorno_Percent': ''
                    })
                    
            else:  # Venda
                # First close any long positions
                remaining_qty = op['quantity']
                
                while remaining_qty > 0 and open_positions and open_positions[0]['is_long']:
                    pos = open_positions[0]
                    close_qty = min(remaining_qty, pos['qty'])
                    
                    # Calculate P&L for long closure
                    pnl = close_qty * (price_per_share - pos['price'])
                    days_held = (op['date'] - datetime.strptime(pos['date'], '%d/%m/%Y')).days
                    
                    fifo_extract.append({
                        'Asset': asset,
                        'Ticker': op['ticker'],
                        'Tipo_Operacao': 'FECHAMENTO_LONG',
                        'Data_Abertura': pos['date'],
                        'Data_Fechamento': op_date,
                        'Quantidade_Abertura': pos['qty'],
                        'Preco_Abertura': f"{pos['price']:.2f}".replace('.', ','),
                        'Quantidade_Fechamento': close_qty,
                        'Preco_Fechamento': f"{price_per_share:.2f}".replace('.', ','),
                        'Tipo_Fechamento': 'TOTAL' if close_qty == pos['qty'] else 'PARCIAL',
                        'PL_Realizado': f"{pnl:.2f}".replace('.', ','),
                        'Valor_Abertura': f"{pos['qty'] * pos['price']:.2f}".replace('.', ','),
                        'Valor_Fechamento': f"{close_qty * price_per_share:.2f}".replace('.', ','),
                        'Dias_Posicao': str(days_held),
                        'Retorno_Percent': f"{(pnl / (close_qty * pos['price'])) * 100:.2f}".replace('.', ',') + '%' if pos['price'] > 0 else '0,00%'
                    })
                    
                    # Update position
                    if close_qty == pos['qty']:
                        open_positions.pop(0)
                    else:
                        open_positions[0]['qty'] -= close_qty
                    
                    remaining_qty -= close_qty
                
                # Add remaining as short position
                if remaining_qty > 0:
                    open_positions.append({
                        'qty': remaining_qty,
                        'price': price_per_share,
                        'date': op_date,
                        'is_long': False
                    })
                    
                    fifo_extract.append({
                        'Asset': asset,
                        'Ticker': op['ticker'],
                        'Tipo_Operacao': 'ABERTURA_SHORT',
                        'Data_Abertura': op_date,
                        'Data_Fechamento': '',
                        'Quantidade_Abertura': remaining_qty,
                        'Preco_Abertura': f"{price_per_share:.2f}".replace('.', ','),
                        'Quantidade_Fechamento': 0,
                        'Preco_Fechamento': '',
                        'Tipo_Fechamento': '',
                        'PL_Realizado': '0,00',
                        'Valor_Abertura': f"{remaining_qty * price_per_share:.2f}".replace('.', ','),
                        'Valor_Fechamento': '',
                        'Dias_Posicao': '',
                        'Retorno_Percent': ''
                    })
    
    print(f"✅ Extrato FIFO gerado com {len(fifo_extract)} registros")
    return fifo_extract

def save_fifo_extract_to_csv(fifo_extract, filename='extrato_fifo_detalhado.csv'):
    """
    Save FIFO extract to CSV file with Brazilian formatting
    """
    if not fifo_extract:
        print("⚠️  Nenhum dado para exportar.")
        return
    
    # Define headers in Portuguese
    headers = [
        'Asset',
        'Ticker',
        'Tipo_Operacao',
        'Data_Abertura',
        'Data_Fechamento',
        'Quantidade_Abertura',
        'Preco_Abertura',
        'Quantidade_Fechamento',
        'Preco_Fechamento',
        'Tipo_Fechamento',
        'PL_Realizado',
        'Valor_Abertura',
        'Valor_Fechamento',
        'Dias_Posicao',
        'Retorno_Percent'
    ]
    
    try:
        import os
        current_dir = os.getcwd()
        full_path = os.path.join(current_dir, filename)
        print(f"💾 Tentando salvar em: {full_path}")
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers, delimiter=';')
            writer.writeheader()
            
            for row in fifo_extract:
                writer.writerow(row)
        
        # Verify file was created
        if os.path.exists(filename):
            file_size = os.path.getsize(filename)
            print(f"✅ Extrato FIFO detalhado salvo em: {filename}")
            print(f"📊 Total de registros: {len(fifo_extract)}")
            print(f"📁 Tamanho do arquivo: {file_size} bytes")
        else:
            print(f"❌ Arquivo não foi criado: {filename}")
            return
        
        # Show summary
        fechamentos = [row for row in fifo_extract if 'FECHAMENTO' in row['Tipo_Operacao']]
        aberturas = [row for row in fifo_extract if 'ABERTURA' in row['Tipo_Operacao']]
        
        print(f"📈 Operações de abertura: {len(aberturas)}")
        print(f"📉 Operações de fechamento: {len(fechamentos)}")
        
        if fechamentos:
            total_pnl = sum(float(row['PL_Realizado'].replace(',', '.')) for row in fechamentos if row['PL_Realizado'] != '0,00')
            print(f"💰 P&L Total Realizado: {total_pnl:,.2f}")
            
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo CSV: {e}")
        import traceback
        traceback.print_exc()

def group_by_asset(operations):
    """
    Group operations by asset using first 12 characters of ticker
    """
    grouped = defaultdict(list)
    
    for op in operations:
        # Use first 12 characters as asset identifier
        asset_key = op['ticker'][:12]
        grouped[asset_key].append(op)
    
    return grouped

def calculate_portfolio_position(operations):
    """
    Calculate current portfolio position with average prices and realized P&L
    Only considers closed positions for P&L calculation
    """
    portfolio = {}
    
    grouped_ops = group_by_asset(operations)
    
    for asset, ops in grouped_ops.items():
        # Sort operations by date for FIFO calculation
        ops.sort(key=lambda x: x['date'])
        
        # Track position using FIFO method
        position_queue = []  # [(quantity, cost_per_share, is_long), ...]
        current_position = 0
        total_realized_pnl = 0
        total_bought_qty = 0
        total_bought_value = 0
        total_sold_qty = 0
        total_sold_value = 0
        
        for op in ops:
            if op['type'] == 'D':  # Compra (Débito)
                total_bought_qty += op['quantity']
                total_bought_value += op['net_value']
                
                if current_position < 0:  # Closing short position
                    # Close short positions first
                    remaining_to_close = min(op['quantity'], abs(current_position))
                    qty_left = op['quantity']
                    
                    while remaining_to_close > 0 and position_queue:
                        queue_qty, queue_cost, is_long = position_queue[0]
                        if not is_long:  # Short position
                            close_qty = min(remaining_to_close, queue_qty)
                            
                            # Calculate P&L: profit when buy price < sell price for closing short
                            pnl = close_qty * (queue_cost - op['net_value'] / op['quantity'])
                            total_realized_pnl += pnl
                            
                            # Update queue
                            if close_qty == queue_qty:
                                position_queue.pop(0)
                            else:
                                position_queue[0] = (queue_qty - close_qty, queue_cost, is_long)
                            
                            remaining_to_close -= close_qty
                            qty_left -= close_qty
                            current_position += close_qty
                    
                    # Add remaining quantity as long position
                    if qty_left > 0:
                        cost_per_share = op['net_value'] / op['quantity']
                        position_queue.append((qty_left, cost_per_share, True))
                        current_position += qty_left
                else:  # Adding to long position or starting long
                    cost_per_share = op['net_value'] / op['quantity']
                    position_queue.append((op['quantity'], cost_per_share, True))
                    current_position += op['quantity']
                    
            else:  # Venda (Crédito)
                total_sold_qty += op['quantity']
                total_sold_value += op['net_value']
                
                if current_position > 0:  # Closing long position
                    # Close long positions first
                    remaining_to_close = min(op['quantity'], current_position)
                    qty_left = op['quantity']
                    
                    while remaining_to_close > 0 and position_queue:
                        queue_qty, queue_cost, is_long = position_queue[0]
                        if is_long:  # Long position
                            close_qty = min(remaining_to_close, queue_qty)
                            
                            # Calculate P&L: profit when sell price > buy price for closing long
                            pnl = close_qty * (op['net_value'] / op['quantity'] - queue_cost)
                            total_realized_pnl += pnl
                            
                            # Update queue
                            if close_qty == queue_qty:
                                position_queue.pop(0)
                            else:
                                position_queue[0] = (queue_qty - close_qty, queue_cost, is_long)
                            
                            remaining_to_close -= close_qty
                            qty_left -= close_qty
                            current_position -= close_qty
                    
                    # Add remaining quantity as short position
                    if qty_left > 0:
                        cost_per_share = op['net_value'] / op['quantity']
                        position_queue.append((qty_left, cost_per_share, False))
                        current_position -= qty_left
                else:  # Adding to short position or starting short
                    cost_per_share = op['net_value'] / op['quantity']
                    position_queue.append((op['quantity'], cost_per_share, False))
                    current_position -= op['quantity']
        
        # Calculate current position cost basis
        cost_basis = 0
        for qty, cost_per_share, is_long in position_queue:
            if is_long:
                cost_basis += qty * cost_per_share
            # For short positions, we don't add to cost basis as they represent liabilities
        
        # Average prices (only for display purposes)
        avg_buy_price = total_bought_value / total_bought_qty if total_bought_qty > 0 else 0
        avg_sell_price = total_sold_value / total_sold_qty if total_sold_qty > 0 else 0
        
        portfolio[asset] = {
            'current_quantity': current_position,
            'total_bought_qty': total_bought_qty,
            'total_sold_qty': total_sold_qty,
            'avg_buy_price': avg_buy_price,
            'avg_sell_price': avg_sell_price,
            'total_invested': total_bought_value,
            'total_received': total_sold_value,
            'realized_pnl': total_realized_pnl,
            'cost_basis': cost_basis,
            'open_positions': len(position_queue)
        }
    
    return portfolio

def calculate_tax_compensation(monthly_pnl):
    """
    Calculate fiscal compensation based on Brazilian tax rules for day trading
    - Starts with previous accumulated loss from PREJUIZO_ACUMULADO_ANTERIOR
    - Negative balance can offset future profits
    - Positive balance assumes DARF payment and resets to zero
    - Only uses final monthly P&L
    """
    # Sort months chronologically
    sorted_months = sorted(monthly_pnl.keys())
    
    # Start with previous accumulated loss
    compensation_balance = PREJUIZO_ACUMULADO_ANTERIOR
    monthly_compensation = {}
    
    print(f"   💼 Iniciando com prejuízo anterior: {format_currency(PREJUIZO_ACUMULADO_ANTERIOR)}")
    
    for month in sorted_months:
        monthly_data = monthly_pnl[month]
        monthly_result = monthly_data['realized_pnl']
        
        # Previous balance (can only be negative or zero)
        previous_balance = compensation_balance
        
        if monthly_result > 0:  # Profit this month
            if compensation_balance < 0:  # Have losses to offset
                # Use previous losses to offset current profit
                offset_amount = min(abs(compensation_balance), monthly_result)
                taxable_profit = monthly_result - offset_amount
                compensation_balance += offset_amount  # Reduces negative balance
                
                monthly_compensation[month] = {
                    'monthly_result': monthly_result,
                    'previous_balance': previous_balance,
                    'offset_used': offset_amount,
                    'taxable_profit': taxable_profit,
                    'tax_due': taxable_profit * 0.15 if taxable_profit > 0 else 0,  # 15% tax rate
                    'new_balance': 0.0 if taxable_profit > 0 else compensation_balance,
                    'status': 'DARF PAGA' if taxable_profit > 0 else 'COMPENSADO'
                }
                
                # If there's taxable profit, assume DARF is paid and balance resets
                if taxable_profit > 0:
                    compensation_balance = 0.0
                    
            else:  # No previous losses, full taxation
                taxable_profit = monthly_result
                tax_due = taxable_profit * 0.15
                
                monthly_compensation[month] = {
                    'monthly_result': monthly_result,
                    'previous_balance': previous_balance,
                    'offset_used': 0.0,
                    'taxable_profit': taxable_profit,
                    'tax_due': tax_due,
                    'new_balance': 0.0,  # Reset after DARF payment
                    'status': 'DARF PAGA'
                }
                
                compensation_balance = 0.0  # Reset after DARF payment
                
        elif monthly_result < 0:  # Loss this month
            # Add loss to compensation balance
            compensation_balance += monthly_result  # monthly_result is negative
            
            monthly_compensation[month] = {
                'monthly_result': monthly_result,
                'previous_balance': previous_balance,
                'offset_used': 0.0,
                'taxable_profit': 0.0,
                'tax_due': 0.0,
                'new_balance': compensation_balance,
                'status': 'PREJUÍZO ACUMULADO'
            }
        else:  # Zero result
            monthly_compensation[month] = {
                'monthly_result': monthly_result,
                'previous_balance': previous_balance,
                'offset_used': 0.0,
                'taxable_profit': 0.0,
                'tax_due': 0.0,
                'new_balance': compensation_balance,
                'status': 'SEM RESULTADO'
            }
    
    return monthly_compensation

def format_currency(value):
    """Format value as Brazilian currency"""
    if value == 0:
        return "0,00"
    elif value > 0:
        return f"{value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    else:
        return f"{value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def calculate_monthly_pnl(operations):
    """
    Calculate monthly profit/loss considering only closed positions
    """
    monthly_pnl = defaultdict(lambda: {
        'realized_pnl': 0,
        'total_bought': 0,
        'total_sold': 0,
        'operations': [],
        'closed_positions': 0
    })
    
    grouped_ops = group_by_asset(operations)
    
    for asset, ops in grouped_ops.items():
        # Sort operations by date for FIFO calculation
        ops.sort(key=lambda x: x['date'])
        
        # Track position using FIFO method per month
        position_queue = []  # [(quantity, cost_per_share, is_long), ...]
        current_position = 0
        
        for op in ops:
            month = op['month']
            
            if op['type'] == 'D':  # Compra
                monthly_pnl[month]['total_bought'] += op['net_value']
                
                if current_position < 0:  # Closing short position
                    remaining_to_close = min(op['quantity'], abs(current_position))
                    qty_left = op['quantity']
                    
                    while remaining_to_close > 0 and position_queue:
                        queue_qty, queue_cost, is_long = position_queue[0]
                        if not is_long:  # Short position
                            close_qty = min(remaining_to_close, queue_qty)
                            
                            # Calculate P&L for closed position
                            pnl = close_qty * (queue_cost - op['net_value'] / op['quantity'])
                            monthly_pnl[month]['realized_pnl'] += pnl
                            monthly_pnl[month]['closed_positions'] += 1
                            
                            # Update queue
                            if close_qty == queue_qty:
                                position_queue.pop(0)
                            else:
                                position_queue[0] = (queue_qty - close_qty, queue_cost, is_long)
                            
                            remaining_to_close -= close_qty
                            qty_left -= close_qty
                            current_position += close_qty
                    
                    # Add remaining as long position
                    if qty_left > 0:
                        cost_per_share = op['net_value'] / op['quantity']
                        position_queue.append((qty_left, cost_per_share, True))
                        current_position += qty_left
                else:  # Adding to long position
                    cost_per_share = op['net_value'] / op['quantity']
                    position_queue.append((op['quantity'], cost_per_share, True))
                    current_position += op['quantity']
                    
            else:  # Venda
                monthly_pnl[month]['total_sold'] += op['net_value']
                
                if current_position > 0:  # Closing long position
                    remaining_to_close = min(op['quantity'], current_position)
                    qty_left = op['quantity']
                    
                    while remaining_to_close > 0 and position_queue:
                        queue_qty, queue_cost, is_long = position_queue[0]
                        if is_long:  # Long position
                            close_qty = min(remaining_to_close, queue_qty)
                            
                            # Calculate P&L for closed position
                            pnl = close_qty * (op['net_value'] / op['quantity'] - queue_cost)
                            monthly_pnl[month]['realized_pnl'] += pnl
                            monthly_pnl[month]['closed_positions'] += 1
                            
                            # Update queue
                            if close_qty == queue_qty:
                                position_queue.pop(0)
                            else:
                                position_queue[0] = (queue_qty - close_qty, queue_cost, is_long)
                            
                            remaining_to_close -= close_qty
                            qty_left -= close_qty
                            current_position -= close_qty
                    
                    # Add remaining as short position
                    if qty_left > 0:
                        cost_per_share = op['net_value'] / op['quantity']
                        position_queue.append((qty_left, cost_per_share, False))
                        current_position -= qty_left
                else:  # Adding to short position
                    cost_per_share = op['net_value'] / op['quantity']
                    position_queue.append((op['quantity'], cost_per_share, False))
                    current_position -= op['quantity']
            
            monthly_pnl[month]['operations'].append({
                'asset': asset,
                'type': op['type'],
                'quantity': op['quantity'],
                'price': op['price'],
                'value': op['net_value']
            })
    
    return monthly_pnl

def main():
    try:
        # Read the trading data from file
        with open('notas_extraidas.txt', 'r', encoding='utf-8') as file:
            file_content = file.read()
    except FileNotFoundError:
        print("❌ Erro: Arquivo 'notas_extraidas.txt' não encontrado!")
        print("Certifique-se de que o arquivo está no mesmo diretório do script.")
        return
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo: {e}")
        return
    
    # Parse operations
    operations = parse_trading_data(file_content)
    
    if not operations:
        print("❌ Nenhuma operação encontrada no arquivo!")
        return
    
    print(f"✅ {len(operations)} operações carregadas do arquivo.")
    
    # Calculate portfolio position
    portfolio = calculate_portfolio_position(operations)
    
    # Calculate monthly P&L
    monthly_pnl = calculate_monthly_pnl(operations)
    
    # Calculate tax compensation
    print("🧮 Calculando compensação fiscal...")
    tax_compensation = calculate_tax_compensation(monthly_pnl)
    print(f"📋 Compensação fiscal calculada para {len(tax_compensation)} meses")
    
    # Debug: Show tax compensation details
    for month, data in tax_compensation.items():
        print(f"   {month}: P&L={data['monthly_result']:.2f}, Saldo={data['new_balance']:.2f}, Status={data['status']}")
    
    # Generate detailed FIFO extract
    print("🔄 Gerando extrato FIFO detalhado...")
    try:
        fifo_extract = generate_fifo_extract(operations)
        print(f"📋 Extrato gerado com {len(fifo_extract)} registros")
        
        # Save FIFO extract to CSV
        save_fifo_extract_to_csv(fifo_extract)
        
    except Exception as e:
        print(f"❌ Erro ao gerar extrato FIFO: {e}")
        import traceback
        traceback.print_exc()
    
    # Display results
    print("\n" + "=" * 120)
    print("ANÁLISE DE CARTEIRA - PETROBRAS")
    print("=" * 120)
    
    # =========================================================================
    # Preparação dos dados para a Tabela 1 (Posição da Carteira) e para o JSON
    # =========================================================================
    portfolio_json_data = []
    for asset, data in portfolio.items():
        pm_compra = f"{data['avg_buy_price']:.2f}" if data['avg_buy_price'] > 0 else ""
        pm_venda = f"{data['avg_sell_price']:.2f}" if data['avg_sell_price'] > 0 else ""
        total_invest = f"{data['total_invested']:,.0f}"
        total_receb = f"{data['total_received']:,.0f}"
        pnl_real = f"{data['realized_pnl']:,.0f}"
        custo_base = f"{data['cost_basis']:,.0f}" if data['current_quantity'] > 0 else ""
        
        portfolio_json_data.append({
            'TICKER': asset,
            'QTD ATUAL': f"{data['current_quantity']:,}",
            'PM COMPRA': pm_compra,
            'PM VENDA': pm_venda,
            'TOTAL INVEST': total_invest,
            'TOTAL RECEB': total_receb,
            'P&L REAL': pnl_real,
            'CUSTO BASE': custo_base
        })

    print("\n📊 POSIÇÃO ATUAL DA CARTEIRA:")
    headers_portfolio = ['TICKER', 'QTD ATUAL', 'PM COMPRA', 'PM VENDA', 'TOTAL INVEST', 'TOTAL RECEB', 'P&L REAL', 'CUSTO BASE']
    
    if TABULATE_AVAILABLE:
        portfolio_display_data = [list(row.values()) for row in portfolio_json_data]
        print(tabulate(portfolio_display_data, headers=headers_portfolio, tablefmt='grid', stralign='center', numalign='right'))
    else:
        print("-" * 120)
        print(f"{headers_portfolio[0]:<12} {headers_portfolio[1]:<10} {headers_portfolio[2]:<12} {headers_portfolio[3]:<12} {headers_portfolio[4]:<15} {headers_portfolio[5]:<15} {headers_portfolio[6]:<12} {headers_portfolio[7]:<15}")
        print("-" * 120)
        for row in portfolio_json_data:
            print(f"{row['TICKER']:<12} {row['QTD ATUAL']:<10} {row['PM COMPRA']:<12} {row['PM VENDA']:<12} {row['TOTAL INVEST']:<15} {row['TOTAL RECEB']:<15} {row['P&L REAL']:<12} {row['CUSTO BASE']:<15}")

    # =====================================================================================
    # Preparação dos dados para a Tabela 2 (Resultado Mensal com Compensação) e para o JSON
    # =====================================================================================
    monthly_json_data = []
    for month in sorted(monthly_pnl.keys()):
        data = monthly_pnl[month]
        tax_data = tax_compensation.get(month, {})
        
        compras = f"{data['total_bought']:,.0f}"
        vendas = f"{data['total_sold']:,.0f}"
        pnl = format_currency(data['realized_pnl'])
        saldo_fiscal = format_currency(tax_data.get('new_balance', 0))
        imposto = format_currency(tax_data.get('tax_due', 0))
        status = tax_data.get('status', '')
        
        monthly_json_data.append({
            'MÊS': month,
            'COMPRAS': compras,
            'VENDAS': vendas,
            'P&L REALIZADO': pnl,
            'POS FECHADAS': data['closed_positions'],
            'SALDO FISCAL': saldo_fiscal,
            'IMPOSTO DEVIDO': imposto,
            'STATUS FISCAL': status
        })

    print("\n📈 RESULTADO POR MÊS COM COMPENSAÇÃO FISCAL:")
    headers_monthly = ['MÊS', 'COMPRAS', 'VENDAS', 'P&L REALIZADO', 'POS FECHADAS', 'SALDO FISCAL', 'IMPOSTO DEVIDO', 'STATUS FISCAL']

    if TABULATE_AVAILABLE:
        monthly_display_data = [list(row.values()) for row in monthly_json_data]
        print(tabulate(monthly_display_data, headers=headers_monthly, tablefmt='grid', stralign='center', numalign='right'))
    else:
        print("-" * 140)
        print(f"{headers_monthly[0]:<10} {headers_monthly[1]:<15} {headers_monthly[2]:<15} {headers_monthly[3]:<15} {headers_monthly[4]:<5} {headers_monthly[5]:<15} {headers_monthly[6]:<15} {headers_monthly[7]:<20}")
        print("-" * 140)
        for row in monthly_json_data:
            print(f"{row['MÊS']:<10} {row['COMPRAS']:<15} {row['VENDAS']:<15} {row['P&L REALIZADO']:<15} {row['POS FECHADAS']:<5} {row['SALDO FISCAL']:<15} {row['IMPOSTO DEVIDO']:<15} {row['STATUS FISCAL']:<20}")
        print("-" * 140)
    
    # Add fiscal summary
    final_balance = 0
    total_taxes = 0
    if tax_compensation:
        final_balance = list(tax_compensation.values())[-1].get('new_balance', 0)
        total_taxes = sum(data.get('tax_due', 0) for data in tax_compensation.values())
    
    # Summary totals
    total_invested = sum(data['total_invested'] for data in portfolio.values())
    total_received = sum(data['total_received'] for data in portfolio.values())
    total_realized_pnl = sum(data['realized_pnl'] for data in portfolio.values())
    total_current_qty = sum(data['current_quantity'] for data in portfolio.values())
    total_cost_basis = sum(data['cost_basis'] for data in portfolio.values())

    # =================================================
    # NOVA SEÇÃO: Salvar os resultados em fiscal_m.json
    # =================================================
    json_output_data = {
        "posicao_atual_carteira": portfolio_json_data,
        "resultado_mensal_com_compensacao_fiscal": monthly_json_data
    }
    
    try:
        with open('fiscal_m.json', 'w', encoding='utf-8') as json_file:
            json.dump(json_output_data, json_file, ensure_ascii=False, indent=4)
        print("\n✅ Relatório fiscal salvo com sucesso no arquivo 'fiscal_m.json'")
    except Exception as e:
        print(f"\n❌ Erro ao salvar o arquivo JSON: {e}")

if __name__ == "__main__":
    main()