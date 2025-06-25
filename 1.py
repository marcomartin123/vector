# fix_fonts.py
import matplotlib.font_manager as fm
import matplotlib

print(f"Local do cache do Matplotlib: {matplotlib.get_cachedir()}")
print("Isso pode levar alguns instantes...")

# A linha mágica que força a reconstrução do cache
fm._rebuild()

print("\nCache de fontes reconstruído com sucesso!")
print("Por favor, reinicie seu programa 'app.py' para ver a mudança.")

# Opcional: Verificar se a fonte agora é reconhecida
font_name_to_check = "TT Interphases Pro Mono Trl"
available_fonts = fm.get_font_names()

if font_name_to_check in available_fonts:
    print(f"\nÓTIMO: A fonte '{font_name_to_check}' foi encontrada pelo Matplotlib.")
else:
    print(f"\nAVISO: A fonte '{font_name_to_check}' ainda não foi encontrada após a reconstrução.")
    print("Verifique se o nome da fonte está digitado exatamente como aparece no seu sistema.")