# 🌊 Modelo de Previsão de Correnteza V6 (Produção)

Este repositório contém a **Versão 6 (V6)** do pipeline de Machine Learning responsável por prever a velocidade (intensidade) da **correnteza de superfície** para horizontes de **+1 hora até +6 horas** à frente. 

A V6 é o modelo oficial desenhado para operar em **produção**. Ela foi construída com foco em robustez, alta confiabilidade e fácil deployment via Google Cloud (Cloud Functions ou localmente de forma agendada).

---

## 🚀 Como Clonar e Rodar (Passo a Passo)

Siga estas instruções para configurar o ambiente e executar a inferência de predição localmente ou em seu servidor.

### 1. Clonar o Repositório
Abra o terminal (ou Git Bash) e rode:
```bash
git clone https://github.com/Estatistica-Praticagem/prev_ml_correnteza.git
cd prev_ml_correnteza
```

### 2. Criar um Ambiente Virtual (Opcional, mas recomendado)
Isole os pacotes Python para este projeto. No Windows:
```bash
python -m venv .venv
.venv\Scripts\activate
```
*(No Linux / Mac, use `source .venv/bin/activate`)*

### 3. Instalar Dependências
O pipeline foca num set minimalista de bibliotecas baseadas em árvores de decisão.
```bash
pip install pandas numpy lightgbm requests google-cloud-bigquery joblib db-dtypes
```
*(Adicione `db-dtypes` para que o pandas possa converter tabelas do BigQuery em DataFrames facilmente.)*

### 4. Configurar as Credenciais do Google Cloud
Para que o `main.py` faça a leitura e a escrita no **BigQuery**, o código espera que exista um arquivo JSON com as chaves da Service Account. 
- Salve sua chave do Google Cloud (GCP) com o nome `chave.json`.
- Atualize a variável `CREDENTIALS_PATH` no arquivo `modelo_V6/main.py` apontando para o local exato da sua chave:
  ```python
  CREDENTIALS_PATH = r"C:\caminho\para\sua\chave.json"
  ```

### 5. Executar o Modelo
Dentro da pasta `modelo_V6`, existe o script `main.py` de inferência:
```bash
cd modelo_V6
python main.py
```
Se tudo rodar com sucesso, você verá no console o modelo carregando o LightGBM, ingerindo dados, inferindo valores, salvando localmente um CSV (`previsoes_v6_last_run.csv`) e enviando o `MERGE` pro BigQuery.

---

## 🧠 Como o Modelo V6 Funciona?

O método escolhido para produção foi o **Multi-Step Recursive Forecasting** usando um modelo baseado em histogramas (`LightGBM`). 

O modelo não prevê apenas uma hora. Ao invés disso, ele prevê um par de horas $t+1$ e $t+2$. E então, **injeta os próprios resultados finais previstos** de volta nos campos defasados (lags) para rodar a previsão dos tempos seguintes ($t+3, t+4$ e dps $t+5, t+6$). Ele roda em *3 rounds*.

1. **Round 1 (Predição Horizontes +1h e +2h)**: Utiliza `100% dados reais e consolidados` vindos do BQ e meteorologia OpenMeteo.  
2. **Round 2 (Predição Horizontes +3h e +4h)**: Encurta o lag1 e lag2 repondo-os com as previsões recém obtidas no "Round 1". Além disso, ele antecipa os *forecasts* oficiais de vento e chuva para gerar as saídas de 3 e 4 horas.
3. **Round 3 (Predição Horizontes +5h e +6h)**: Mesma lógica de alimentação em cascata, usando os *outputs* do "Round 2".

---

## 📊 Estrutura de Dados Lidos e Previstos

Para gerar inferências precisas, a ferramenta une as condições ambientais, defasagens móveis (MA) e chuva.

### QUAIS COLUNAS O MODELO LÊ?

| Categoria | Coluna Lida | Descrição / Transformação | Fonte |
| :--- | :--- | :--- | :--- |
| **Maré** | `previsao` | Altura astronômica da maré prevista. | BigQuery |
| **Maré** | `altura_mare` | Altura real (medida) da maré. | BigQuery |
| **Clima (Passado)** | `hi_temp`, `out_hum`, `wind_speed`, `vento_num`, `bar` | Variáveis capturadas de estações metereológicas da hora atual, como umidade, velocidade e ângulo do vento, etc. | BigQuery |
| **Astro** | `fase_lua` | Fase lunar (valor numérico). | BigQuery |
| **Correnteza** | `intensidade_3m_kt`, `intensidade_6m_kt`, `intensidade_superficie_kt` | Velocidade da corrente em nós (knots). Transformada aplicando o sinal de enchente (+) ou vazante (-).  | BigQuery |
| **Correnteza** | `direcao_3m_deg`, `direcao_6m_deg`, `direcao_superficie_deg` | Ângulo de direçao. Utilizada internamente, entre eixos fixos, para determinar Enchente ou Vazante. | BigQuery |
| **Chuva Cidades** | `rain_PORTO_ALEGRE` etc. (9 cidades total) | Precitação da hora. São feitas médias de longo prazo das chuvas (12h, 24h, 48h) de 9 pontos da bacia. | API Open-Meteo |
| **Clima (Futuro)** | Vento, humidade, pressão, temperatura (*Forecast*) | Variáveis futuras previstas da API injetadas apenas nos *Rounds 2 e 3*. | BigQuery (via OW) |

Internamente, todas essas colunas bases geram **188 features** a cada execução através de matrizes dinâmicas de médias móveis (`_ma3`, `_ma6`, etc.) e recuos horários (`_lag1`, `_lag2`, etc.).

### O QUE O MODELO PREVÊ (SAÍDA)?

O modelo V6 tem foco total na **intensidade da superfície**, convertida no momento da gravação no pipeline para a intensidade prevista nos próximos passos em nós.

| Coluna de Saída (Output BQ) | Descrição e Comportamento |
| :--- | :--- |
| `datahora_alvo` | Data e hora em que a correnteza *deverá* acontecer ($t+x$).  |
| `correnteza_superficie_prevista_dinamica` | A previsão a todo o momento gerada do modelo. A gravação ocorre por script de `MERGE` de 15 em 15 minutos: conforme nos aproximamos da "hora chave", o modelo regrava essa coluna aumentando a confiança sem duplicar linhas. |
| `correnteza_superficie_prevista_primeiro_calculo` | Coluna fixa ("carimbo") de qualitação e auditoria: este campo trava (e não é alterado nunca mais) exatamente quando restam apenas **~2 horas pro evento da datahora alvo ocorrer**, permitindo análise do quão assertiva é a precisão com a distância de antecedência de 2 horas. |

---

## 🛠 Arquivos Cruciais no `modelo_V6`

Ao navegar pela pasta de `modelo_V6`, os principais arquivos são:
* `main.py` -> Seu core central. Buscador, tratador, criador do motor de feature lag, preditor de Rounds e executor do banco.
* `modelo_LightGBM.joblib` -> O "cérebro" treinado contendo as matrizes de árvore e pesos.
* `scaler_X.joblib` / `scaler_y.joblib` -> Funções salvas que preparam os dados numéricos antes de entrar no modelo (padronizam suas grandezas) e re-transformam as saídas numéricas para velocidades de nós reais de novo.
* `DOCUMENTACAO_V6.md` -> Manual legado estrito em markdown.

Qualquer ajuste no modelo, você deve puxar `modelo_V5` (notebook de treino), rodar para treinar com novas tabelas e sobreescrever os `.joblib` aqui nesta pasta V6.
