import json

with open('novo_modelo_correnteza.py', 'r', encoding='utf-8') as f:
    content = f.read()

cells = []
blocks = content.split('# %%')

for block in blocks:
    if not block.strip(): continue
    lines = block.strip('\n').split('\n')
    
    if lines[0].strip() == '[markdown]':
        md_lines = []
        for line in lines[1:]:
            if line.startswith('# '): md_lines.append(line[2:] + '\n')
            elif line.startswith('#'): md_lines.append(line[1:] + '\n')
            else: md_lines.append(line + '\n')
        if md_lines: md_lines[-1] = md_lines[-1].rstrip('\n')
        cells.append({'cell_type': 'markdown', 'metadata': {}, 'source': md_lines})
    else:
        source_lines = [line + '\n' for line in lines]
        if source_lines: source_lines[-1] = source_lines[-1].rstrip('\n')
        cells.append({'cell_type': 'code', 'execution_count': None, 'metadata': {}, 'outputs': [], 'source': source_lines})

nb = {'cells': cells, 'metadata': {'kernelspec': {'display_name': 'Python 3', 'language': 'python', 'name': 'python3'}}, 'nbformat': 4, 'nbformat_minor': 4}

with open('novo_modelo_correnteza.ipynb', 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=2, ensure_ascii=False)
