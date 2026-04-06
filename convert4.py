import json
with open(r'C:\Users\LUIS\Desktop\previsoes_dados_agua\modelo_V4\novo_modelo_correnteza_V4.py', 'r', encoding='utf-8') as f:
    content = f.read()
cells = []
for block in content.split('# %%'):
    if not block.strip(): continue
    lines = block.strip('\n').split('\n')
    if lines[0].strip() == '[markdown]':
        md = [l[2:]+'\n' if l.startswith('# ') else l[1:]+'\n' if l.startswith('#') else l+'\n' for l in lines[1:]]
        if md: md[-1] = md[-1].rstrip('\n')
        cells.append({'cell_type':'markdown','metadata':{},'source':md})
    else:
        src = [l+'\n' for l in lines]
        if src: src[-1] = src[-1].rstrip('\n')
        cells.append({'cell_type':'code','execution_count':None,'metadata':{},'outputs':[],'source':src})
nb = {'cells':cells,'metadata':{'kernelspec':{'display_name':'Python 3','language':'python','name':'python3'}},'nbformat':4,'nbformat_minor':4}
with open(r'C:\Users\LUIS\Desktop\previsoes_dados_agua\modelo_V4\novo_modelo_correnteza_V4.ipynb','w',encoding='utf-8') as f:
    json.dump(nb, f, indent=2, ensure_ascii=False)
print('Done')
