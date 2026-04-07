# 🌊 Modelo de Previsão de Correnteza V7 (Produção)

Este repositório contém a **Versão 7 (V7)** do pipeline de Machine Learning responsável por prever a velocidade (intensidade) da **correnteza de superfície** para o Porto de Rio Grande em horizontes de **+1 hora até +6 horas** à frente. 

A V7 é o state-of-the-art do nosso sistema operacional. Diferente de todas as gerações antigas, a V7 é dotada da mecânica de **Leads Meteorológicos**, lendo rigorosamente como a atmosfera se portará nas próximas 12 horas e integrando esse "Conhecimento do Futuro" com "Inércias do Passado", viabilizando a mais pura precisão matemática focada em hidrodinâmica oceânica e costeira.

---

## 🚀 Como Clonar e Rodar (Passo a Passo)

Siga estas instruções para configurar o ambiente e executar a inferência preditiva em Cloud ou local.

### 1. Clonar o Repositório
Abra o terminal (ou Git Bash) e rode:
```bash
git clone https://github.com/luisin-py/prev_correnteza.git
cd prev_correnteza
```

### 2. Criar um Ambiente Virtual (Recomendado)
```bash
python -m venv .venv
.venv\Scripts\activate
```

### 3. Instalar Dependências
O pipeline exige este set fundamental de bibliotecas de manipulação, redes de árvores e chaves DB:
```bash
pip install pandas numpy lightgbm requests google-cloud-bigquery pandas-gbq joblib db-dtypes
```

### 4. Configurar as Credenciais do Google Cloud
Para que o BigQuery entregue o Acervo da Boia e aceite as predições de saída:
- Salve sua chave do Google Cloud (GCP) com o nome `chave.json`.
- Atualize a rotina no início dos arquivos Python na pasta `modelo_V7` ajustando a variável:
  ```python
  CREDENTIALS_PATH = r"C:\caminho\para\sua\chave.json"
  ```

### 5. Executar a Inferência em Produção
O Motor de Deploy Inteligente:
```bash
cd modelo_V7
python main_v7.py
```
Ao rodar com sucesso, você verá em seu console Log o *LightGBM* processar os vetores meteorológicos do futuro da Baía, cruzar com a inércia, e acionar o `MERGE` de +6 Horas para o BigQuery na base `previsoes_oficiais`.

---

## 🧠 Como o Modelo V7 Funciona?

O método escolhido para produção rompeu o paradigma das adivinhações cegas em rodadas. Criamos uma heurística de **Walk-Forward com Leads Progressivos** embarcada num super algoritmo Árvore (`LightGBM`). 

1. **A Máquina do Tempo (Fator Lead):** Em vez de jogar fora o tempo, assim que o script inicializa, ele se conecta via APIs Globais (`Open-Meteo` e `OpenWeather OneCall`) resgatando exatamente quantos "nós" e quantos "milímetros" teremos no futuro da barra (até +12 horas no futuro).
2. **Featurização Dinâmica:** Para gerar os Lags e as MAs (Médias Móveis), injetamos essas Horas do Clima diretamente no Pandas por baixo da linha real. E utilizando índices deslocáveis em `shift(-lead)`, a linha de extração absorve todos estes climas futuros perfeitamente ancorada em `t0`.
3. **Walk-Forward Recursivo:** Após fazer a previsão ultra-segura para a Hora + 1 de intensidade de água, o sistema injeta magicamente a sua própria resposta no campo vazio à frente, "anda" um passo temporal para cima usando `for-loop`, e roda a matemática usando a Água Simulada + Clima Genuíno (Leads), destravando a porta sucessiva para o T+2 ... T+6 sem degradações violentas das versões legadas.

---

## 📊 Estrutura de Predição de Carga (O que a rede enxerga?)

Para que a LightGBM gere o Output vetorial com pontas de assertividade batendo os `0.29 Kt` Absolutos (MAE), alimentam-na **258 Features**.

| Categoria | Dinâmica de Feature | Transformação Aplicada no Script |
| :--- | :--- | :--- |
| **Boia (água)** | Correnteza, Altura, Pressão passadas. | `_lag1` até `_lag5`. |
| **Ondulatória** | Tendência do Estuário. | `_ma3` e `_ma6` (Médias Móveis Curtas). |
| **Bússola** | Ventos e Marés Cíclicas (`0` a `360º`). | Fatiamento Categórico Matemático (De `0` até `15` quadrantes estritos). |
| **Carga de Bacia** | Chuvas de 9 Cidades Gaúchas ligadas à foz. | Interseções Densas `_ma12`, `_ma24`, `_ma48`. |
| **O Futuro Atm.** | Pressão, Vento e Chuva que CAIRÃO no farol. |  A cereja do Bolo: Vetores projetados como `_lead1` a `_lead5` na cabeça do tempo ativo. |

### Gravação BigQuery (Merge de Inserção Oficial)

Ao terminar as equações o `main_v7.py` joga 6 colunas do futuro formatadas num Dataframe e as mescla assim:

| Coluna de Saída (Output BQ Oficial) | Descrição e Comportamento |
| :--- | :--- |
| `datahora_alvo` | Data e hora exata em que o prático lidará com a navegação do navio ($t+x$). |
| `correnteza_superficie_prevista_dinamica` | Eterna re-previsão gerada durante os *Cron Jobs* a todo momento, se aprimorando à medida que a hora chega. |
| `correnteza_superficie_prevista_primeiro_calculo` | Coluna Fixa "Cold-Stamp" (Status da Praticagem). Esta coluna é gravada EXCLUSIVAMENTE, num update engessado, no timing em que a distância daquele lote bater a diferença de $2\text{Hrs}$. Protegendo uma auditoria estrita de assertividade pra dashboard de Business Intelligence. |

---

## 👨‍💻 Arquitetura de Pastas de Serviço (`modelo_V7`)

Os módulos foram apartados no princípio MLOps para facilitar o DevOps:
* `build_features_V7.py` -> A Fábrica. Puxa via nuvem, gera matriz imensa de 260 dimensões e *Upserts* na tabela de treino frio BQ para cientistas reajustarem no futuro.
* `train_lightgbm_from_bq_V7.py` -> Baixa o esqueleto cru do Build e roda a descida do Gradiente em GPU/CPU da rede LightGBM, serializando e salvando o modelo e o Scaler na pata `joblib`.
* `main_v7.py` -> O Operário Implacável. (Injetor e Preditor Diário para subida na Praticagem Real).
* `DOCUMENTACAO_V7.md` -> Livro Branco de Engenheiros com detalhes precisos e densos da operação modular. 
