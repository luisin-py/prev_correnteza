# 📘 Documentação do Pipeline de Previsão de Correnteza

## Visão Geral

Este projeto consiste em um pipeline de Machine Learning para prever a **intensidade da correnteza** (em nós — kt) em diferentes profundidades de coluna d'água. Os dados de entrada são extraídos do BigQuery e enriquecidos com dados meteorológicos externos (chuva, vento, maré, fase lunar, etc.).

O pipeline evoluiu por 4 versões, cada uma adicionando melhorias arquitetônicas e conceituais.

---

## Estrutura de Arquivos

```
previsoes_dados_agua/
│
├── novo_modelo_correnteza_V1.py       # Versão 1 (baseline)
│
├── modelo_V2/
│   ├── novo_modelo_correnteza_V2.py   # Versão 2
│   └── novo_modelo_correnteza_V2.ipynb
│
├── modelo_V3/
│   ├── novo_modelo_correnteza_V3.py   # Versão 3
│   └── novo_modelo_correnteza_V3.ipynb
│
├── modelo_V4/
│   ├── novo_modelo_correnteza_V4.py   # Versão 4
│   └── novo_modelo_correnteza_V4.ipynb
│
├── modelo_V5/
│   ├── novo_modelo_correnteza_V5.py   # Versão 5 (atual)
│   └── novo_modelo_correnteza_V5.ipynb
│
└── 2026_kevi/
    └── chave.json                     # Service Account BigQuery (não compartilhar)
```

---

## Versão 1 — Baseline

**Arquivo:** `novo_modelo_correnteza_V1.py`

### O que faz
Pipeline inicial que estabelece a fundação do projeto:
- Autenticação com a Service Account local (`chave.json`) para conexão ao BigQuery
- Leitura da tabela `ML.df_data_20260328`
- Pré-processamento básico baseado no notebook original `ML_from_bigquery_correnteza_2026.ipynb`
- Feature engineering: **Lags** (1 a 5 períodos) e **Médias Móveis** (`ma3`, `ma6`) para todas as variáveis base
- **2 variáveis alvo:** `intensidade_3m_kt` e `direcao_3m_deg` (em graus)
- Divisão temporal **80% Treino / 10% Validação / 10% Teste**
- Seeds fixadas (`random=42`, `numpy=42`, `tensorflow=42`) para reprodutibilidade
- Salvamento de modelos localmente: `.joblib` para árvores, `.keras` para redes neurais

### Modelos treinados
| Fase | Modelos |
|------|---------|
| Fase 1 — Árvores | Gradient Boosting, XGBoost, LightGBM, CatBoost |
| Fase 2 — Deep Learning | MLP (Multilayer Perceptron) |
| Fase 3 — Deep Learning | LSTM |

### Arquitetura LSTM (V1)
- `Input(shape=(1, features))` — apenas 1 timestep (reshape simples)
- Bloco denso profundo herdado da MLP: 220→200→150→100→80→30→10→5→2
- **Sem Dropout**, **sem sliding windows reais**

### Outputs
| Arquivo | Descrição |
|---------|-----------|
| `metricas_modelos.csv` | Tabela comparativa de métricas |
| `modelo_*.joblib` | Modelos de árvore salvos |
| `modelo_MLP.keras` | Rede MLP salva |
| `modelo_LSTM.keras` | Rede LSTM salva |

---

## Versão 2 — Múltiplas Profundidades

**Arquivo:** `modelo_V2/novo_modelo_correnteza_V2.py`

### O que mudou em relação à V1
| Aspecto | V1 | V2 |
|---------|----|----|
| Tabela BigQuery | `ML.df_data_20260328` | `ML.xtrain_horario_t_2026` |
| Targets | 2 (intensidade + direção, apenas 3m) | **6** (intensidade + direção nas 3 profundidades: 6m, superfície, 3m) |
| MAs para chuva | Nenhuma | MA 12h, 24h, 48h para colunas de chuva/precipitação |
| Eixo X nos gráficos | Índice numérico | **Datas reais** (`datahora`) |
| Pasta de output | Raiz do projeto | `modelo_V2/` (organizado) |

### Arquitetura LSTM (V2) — Corrigida
- `Input(shape=(6, features_base))` — **Sliding Windows reais** com `timesteps=6`
- Função `create_lstm_sequences()` que retroage nos dados originais não-lagados
- Arquitetura drasticamente simplificada: `LSTM(128)` → `Dropout(0.2)` → `Dense(64)` → `Dropout(0.2)` → `Dense(32)` → `Dense(6)`
- **Dropout adicionado** para combater overfitting

### Outputs
Salvos em `modelo_V2/`:
- `base_treinamento.csv`
- `metricas_modelos_v2.csv`
- `modelo_*.joblib`, `modelo_MLP.keras`, `modelo_LSTM_V2.keras`
- `comparativo_previsoes.png`

---

## Versão 3 — Codificação Enchente/Vazante

**Arquivo:** `modelo_V3/novo_modelo_correnteza_V3.py`

### Motivação
A direção em graus (0–360°) é difícil de aprender por redes neurais. Em um canal, o fluxo é essencialmente binário: **Enchente** (entrando) ou **Vazante** (saindo). A codificação pelo sinal da intensidade simplifica radicalmente o problema.

### O que mudou em relação à V2
| Aspecto | V2 | V3 |
|---------|----|----|
| Targets | 6 (3 intensidades + 3 direções) | **3** (apenas intensidades sinalizadas) |
| Direção | Alvo a ser previsto | Mantida como **feature de entrada** (lags/MAs) |
| Codificação do fluxo | Sem codificação | **Intensidade negativa = Vazante** (dir 90°–270°), positiva = Enchente |

### Transformação Direcional
```python
mask_vazante = (df[dir_col] > 90) & (df[dir_col] < 270)
df.loc[mask_vazante, int_col] *= -1
```
> Baseado na Rosa dos Ventos: Sul (90°–270°) = Vazante para o mar aberto.

### Benefícios
- Métrica única e escalar por profundidade (positivo/negativo)
- Modelos convergem mais rápido (menos targets)
- Visualização direta: cruzamento do zero = mudança de maré

### Outputs
Salvos em `modelo_V3/`:
- `base_treinamento.csv`
- `metricas_modelos_v3.csv`
- `modelo_*.joblib`, `modelo_MLP.keras`, `modelo_LSTM_V3.keras`
- `comparativo_previsoes_v3.png`

---

## Versão 4 — Walk-Forward + Chuva Open-Meteo

**Arquivo:** `modelo_V4/novo_modelo_correnteza_V4.py`

### Motivação
A avaliação estática (prever todo o conjunto de teste de uma vez) não reflete o uso real. Na operação real, o modelo prevê 2 horas, recebe o dado real, e prevê as próximas 2 horas. Além disso, chuvas horárias de múltiplas cidades são incorporadas como feature externa.

### O que mudou em relação à V3
| Aspecto | V3 | V4 |
|---------|----|----|
| Dados de chuva | Colunas padrão do BQ | **Open-Meteo API**: chuva horária de 9 cidades do RS (2020–hoje) |
| Targets | Intensidades em t atual | **t+1 e t+2** (2 horizontes simultâneos, 6 outputs: 3 profundidades × 2 horizontes) |
| Avaliação | Estática (teste completo de uma vez) | **Walk-Forward**: janela de 2h cega → ingere real → avança |
| Métricas | Por modelo, por target | Por modelo, por target, **por horizonte** (+1h e +2h separados) |

### Dados de Chuva (Open-Meteo)
Cidades incluídas:
- Porto Alegre, Canoas, São Leopoldo, Novo Hamburgo
- Gravataí, Santa Maria, Cachoeira do Sul, Santa Cruz do Sul, Rio Grande

MAs específicas para chuva: 12h, 24h e 48h (captura inércia hidrológica)

### Walk-Forward Evaluation
```
Período: Validação + Teste (20% mais recentes dos dados)

Loop (passo=2 horas):
  1. Pegar X das próximas 2 horas (sem ver os Y reais)
  2. Prever: modelo retorna y_pred[h+1] e y_pred[h+2]
  3. Salvar pred vs real
  4. Avançar 2 horas (revelar os dados reais)
  5. Repetir
```

### Outputs
Salvos em `modelo_V4/`:
- `base_treinamento_v4.csv`
- `walk_forward_predictions.csv` — todas as previsões individuais com datahora
- `metricas_walk_forward_v4.csv` — métricas separadas por modelo + horizonte (+1h / +2h)
- `modelo_*.joblib`, `modelo_MLP.keras`, `modelo_LSTM_V4.keras`
- `walkforward_h1h.png` e `walkforward_h2h.png` — plots com datas no eixo X

---

## Versão 5 — Retroalimentação Recursiva até +6h

**Arquivo:** `modelo_V5/novo_modelo_correnteza_V5.py`

### Motivação
A V4 avaliava os modelos prevendo 2 horas e depois consumindo dados **reais** para a próxima janela. Isso não reflete o uso real: na prática, os dados de correnteza futura não existem. A V5 simula o cenário real onde o modelo alimenta suas próprias previões de correnteza como entrada para as próximas rodadas.

### O que mudou em relação à V4
| Aspecto | V4 | V5 |
|---------|----:|----:|
| Treinamento | Multi-step h1/h2 | Idêntico |
| Avaliação | Walk-forward 2h, consome dados reais entre janelas | **Motor Recursivo 6h** (3 rounds de 2h) |
| Lags entre janelas | Dados reais do BQ | **Lags de correnteza = previões anteriores** |
| Meteo entre janelas | Dados reais | **Meteo real futuro** (simula previsão do tempo) |
| Horizontes de métrica | +1h e +2h | **+1h até +6h** |

### Motor de Retroalimentação (Seção 11)
```
Round 1  →  Dados Reais(t)  →  Prevê t+1, t+2
                                      ↓
Round 2  →  Lags corr = [t+1, t+2 previstos]   →  Prevê t+3, t+4
             + Meteo Real t+3/t+4 (previsão do tempo)
                                      ↓
Round 3  →  Lags corr = [t+3, t+4 previstos]   →  Prevê t+5, t+6
             + Meteo Real t+5/t+6
```

### Outputs
Salvos em `modelo_V5/`:
- `base_treinamento_v5.csv`
- `walk_forward_recursive_v5.csv` — todas as previões +1h a +6h com datahora
- `metricas_recursivas_v5.csv` — métricas por modelo × horizonte (6 linhas por modelo)
- `degradacao_acuracia_v5.png` — curva de queda de acurácia de +1h → +6h por modelo
- `comparativo_{profundidade}_v5.png` — +1h vs +6h lado a lado

---

## Comparativo Geral das Versões

| Feature | V1 | V2 | V3 | V4 | V5 |
|---------|:--:|:--:|:--:|:--:|:--:|
| Tabela BigQuery | `df_data_20260328` | `xtrain_horario_t_2026` | ✓ | ✓ | ✓ |
| N° de Targets | 2 | 6 | 3 | 6 (3×2h) | 6 (3×2h) |
| Profundidades previstas | 1 | 3 | 3 | 3 | 3 |
| Direção como target | ✓ | ✓ | ✗ | ✗ | ✗ |
| Codificação ±Vazante | ✗ | ✗ | ✓ | ✓ | ✓ |
| Chuva Open-Meteo | ✗ | ✗ | ✗ | ✓ | ✓ |
| MAs longas para chuva | ✗ | ✓ (12/24/48h) | ✓ | ✓ | ✓ |
| LSTM Sliding Windows | ✗ | ✓ ts=6 | ✓ | ✓ | ✓ |
| Dropout na LSTM | ✗ | ✓ | ✓ | ✓ | ✓ |
| Datas no eixo X | ✗ | ✓ | ✓ | ✓ | ✓ |
| Walk-Forward 2h (com dados reais) | ✗ | ✗ | ✗ | ✓ | ✓ |
| Multi-Step (t+1, t+2) | ✗ | ✗ | ✗ | ✓ | ✓ |
| **Motor Recursivo +6h** | ✗ | ✗ | ✗ | ✗ | **✓** |
| **Métricas por horizonte +1h→+6h** | ✗ | ✗ | ✗ | ✗ | **✓** |
| **Retroalimentação de lags** | ✗ | ✗ | ✗ | ✗ | **✓** |
| Pasta isolada de output | ✗ | ✓ V2 | ✓ V3 | ✓ V4 | ✓ V5 |

---

## Requisitos

Todas as versões requerem o ambiente virtual `.venv` com os pacotes:

```bash
pip install pandas numpy matplotlib google-auth pandas-gbq \
            scikit-learn xgboost lightgbm catboost tensorflow joblib requests
```

---

## Configuração de Autenticação

Todas as versões utilizam autenticação via **Service Account** local:

```python
CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
```

> ⚠️ **Importante:** O arquivo `chave.json` contém credenciais sensíveis. Nunca o adicione a repositórios públicos nem o compartilhe.
