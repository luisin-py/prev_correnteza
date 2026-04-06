# 📘 Documentação Técnica Detalhada: Modelo de Correnteza V6

## 1. Introdução
A **Versão 6 (V6)** do Modelo de Predição de Correnteza é a primeira versão totalmente operacional para **produção**. Ela foi desenvolvida para rodar em ciclos de 15 minutos, fornecendo previsões de curtíssimo prazo (+1h a +6h) para a intensidade da correnteza na superfície.

Esta versão prioriza a **robustez e facilidade de deploy**, eliminando dependências pesadas (como TensorFlow) e focando em modelos baseados em histogramas (LightGBM).

---

## 2. Ingestão e Mapeamento de Dados

O modelo V6 atua como uma ponte entre o `Warehouse Tratado` e o `Modelo de Machine Learning`. Abaixo, o detalhamento do mapeamento das fontes de dados para as features do modelo:

### 2.1 Mapeamento SQL (Histórico BigQuery)
Fonte: `wherehouse_tratado.mestre_hour_tratada`

| Coluna Original (BQ) | Nome no Modelo (V5/V6) | Descrição |
| :--- | :--- | :--- |
| `timestamp_br` | `datahora` | Referência temporal (Local BR) |
| `altura_prev_getmare` | `previsao` | Altura da maré prevista astronomicamente |
| `sensacaotermica` | `hi_temp` | Sensação térmica (proxy p/ temperatura externa) |
| `umidade` | `out_hum` | Umidade relativa externa |
| `ventointensidade` | `wind_speed` | Velocidade do vento |
| `ventonum` | `vento_num` | Direção do vento convertida p/ número |
| `pressao` | `bar` | Pressão barométrica |
| `tipo` | `fase_lua` | Identificador numérico da fase da lua |
| `direcao_superficie` | `direcao_superficie_deg` | Ângulo da correnteza na superfície |
| `intensidade_superficie` | `intensidade_superficie_kt` | Velocidade da correnteza (knots) |
| `altura_real_getmare` | `altura_mare` | Altura da maré observada (sensores) |

### 2.2 Estimativa de Momento (Tabela 5min)
Fonte: `wherehouse_tratado.mestre_5min_linear`
*   **Lógica**: Como os dados horários podem demorar a consolidar, o script calcula a média (`AVG`) de todos os registros da hora atual disponíveis na tabela de 5 minutos.
*   **Fallback**: Se a tabela de 5 minutos estiver vazia p/ a hora atual, o script utiliza o último registro consolidado da tabela de hora (1h de atraso).

---

## 3. Pré-Processamento e Engenharia de Features

### 3.1 Transformação Direcional (Signed Intensity)
Para o modelo aprender padrões de **Enchente vs Vazante**, a intensidade (sempre positiva nos sensores) é sinalizada:
*   Se `0 < direcao < 90` ou `270 < direcao < 360` → Direção Norte (geralmente **Enchente**).
*   Se `90 < direcao < 270` → Direção Sul (geralmente **Vazante**).
*   **Ação**: Valores na zona Sul têm sua intensidade multiplicada por `-1`.

### 3.2 Geração de Janelas Móveis (Sliding Windows)
O modelo V6 gera internamente **188 features** a cada rodada:
1.  **Lags (1 a 5 horas)**: Retornos históricos de todas as 23 variáveis base.
2.  **Médias Móveis (MA3, MA6)**: Média das últimas 3 e 6 horas para as variáveis de correnteza e meteorologia.
3.  **Médias Móveis de Chuva (MA12, MA24, MA48)**: Médias de longo prazo para as 9 cidades monitoradas pela Open-Meteo, capturando o efeito de escoamento tardio na bacia.

---

## 4. Motor de Inferência Recursiva

Diferente de uma predição única, a V6 utiliza o método **Multi-Step Recursive**. O modelo LightGBM prevê um passo alternado ($h+1$ e $h+2$).

### Round 1: Horizonte +1h e +2h
*   Utiliza dados 100% reais (BQ + Open-Meteo).
*   Gera as primeiras previsões.

### Round 2: Horizonte +3h e +4h
*   **Injeção de Previsão**: Os valores previstos no Round 1 são inseridos nas colunas de `Lag1` e `Lag2` da correnteza.
*   **Injeção Meteo**: O script busca no Forecast do OpenWeather o vento/pressão previstos para $t+3h$ e injeta no vetor de features.

### Round 3: Horizonte +5h e +6h
*   Alimenta os `Lags` com as previsões do Round 2.
*   Injeta o clima previsto para $t+5h$.

---

## 5. Regras de Escrita no BigQuery (Estratégia MERGE)

A V6 não apenas insere dados, ela gerencia o ciclo de vida da previsão através de um `MERGE` atômico:

```sql
MERGE `previsoes_oficiais` T USING `temp_previsoes_run` S ...
```

### 5.1 Previsão Dinâmica
A coluna `correnteza_superficie_prevista_dinamica` é atualizada em toda rodada (a cada 15 min). À medida que o tempo passa, essa previsão torna-se mais precisa, pois a janela de dados reais $t_0$ aproxima-se da hora alvo.

### 5.2 Validação (Regra da 2ª Hora)
A coluna `correnteza_superficie_prevista_primeiro_calculo` é o "print" do modelo.
*   **Condição**: `IF(primeiro_calculo IS NULL AND TIMESTAMP_DIFF(alvo, agora, MINUTE) BETWEEN 105 AND 135)`
*   **Significado**: O valor é gravado apenas quando a previsão está a exatamente **2 horas de distância**. Uma vez gravado, esse valor **nunca mais muda**, servindo como base fixa para medir o RMSE/MAE de curto prazo do modelo após o evento real ocorrer.

---

## 6. Configurações de Produção e Deploy

### 6.1 Requisitos de Infraestrutura
*   **Google Cloud Function**: Recomendado 512MB a 1GB de RAM.
*   **Timeout**: 120 - 300 segundos (devido às chamadas de API externas).
*   **Trigger**: Cloud Scheduler (cron: `*/15 * * * *`).

### 6.2 Lista de Arquivos (Checklist de Deploy)
1.  `main.py`: Código fonte unificado.
2.  `requirements.txt`: Dependências (`lightgbm`, `pandas`, `google-cloud-bigquery`, `joblib`).
3.  `chave.json`: Credenciais da Service Account.
4.  `scaler_X.joblib`, `scaler_y.joblib`: Normalizadores salvos no treinamento.
5.  `modelo_LightGBM.joblib`: Pesos do modelo vencedor.

---
**Documentação revisada em: 06/04/2026**
