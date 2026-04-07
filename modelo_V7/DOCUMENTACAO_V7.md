# Documentação Técnica: Arquitetura do Modelo de Predição de Correnteza (V7)

Este documento descreve as especificações técnicas, engenharia de dados (Feature Engineering) e o pipeline de deploy de Machine Learning da Arquitetura V7 para predição vetorial e hidrodinâmica do Porto de Rio Grande.

---

## 1. Arquitetura Analítica (Lags e Leads)

O modelo V7 resolve a predição da intensidade das correntes por modelagem multivariada baseada em séries temporais. Para compor o vetor de inferência (instante `T0`), o algoritmo utiliza três classes de features simultâneas:

1. **Features Temporais Regressivas (LAGs e MAs):** Deslocamentos retrospectivos (`t-1` a `t-5`) da intensidade e direções da própria correnteza e dos sensores passados. Utilizadas para capturar as perturbações inerciais do estuário.
2. **Features de Estado (T0):** As condições físicas exatas marcadas pelo sensor hidrodinâmico no timestamp atual (como Temperatura, Pressão, Cota).
3. **Features Temporais Progressivas (LEADs):** Integração preditiva por *Future Horizons*. Modelos matemáticos de escoamento possuem intensa correlação com forçamentos eólicos e barométricos paralelos/futuros. A V7 consolida previsões exatas das APIs (`t+1` a `t+5`) para Chuva, Vento e Pressão como features intrínsecas acopladas ao vetor central `T0`.

---

## 2. Componentes e Rotinas do Sistema (Pipelines)

O sistema segue padrões estritos de MLOps dividindo O processamento Batch do processamento em Streaming (Produção/Inferência). O pipeline fragmenta-se em três scripts autônomos.

### Módulo A: Pipeline de Treinamento e ETL (`build_features_V7.py`)
Responsável pelas rotinas diárias/semanais de ETL e por exportar matrizes consolidadas (`Train/Test Dataframe`) direto no Data Warehouse (`BigQuery`).

**Fluxo Técnico:**
- **Ingestão Externa:** Requisita o endpoint de arquivo histórico (`archive-api.open-meteo.com`) convertendo os subvolumes pluviométricos de 9 bacias em dataframes indexados temporais a partir do ano-base de 2020.
- **Join (Consolidação):** Unificação entre as métricas passadas da Praticagem (`mestre_hour_tratada`) no BigQuery via chave `datahora_h`.
- **Discretização Setorial:** A variável rotacional contínua dos ventos e correntezas numéricas (`0` a `360` graus) é subdividida uniformemente em `16` quadrantes categóricos (`22.5` graus/setor) otimizando divisões de folha por árvores de decisão.
- **Tratamento de Assimetria Escalar:** Isolação pura do Módulo Direcional. Variáveis de estado como "Enchente" / "Vazante" (Sinais de Vetor) têm seus fatores multiplicados em condicionalidade por ângulo de entrada para retornar a Força Vetorial Absoluta.
- **Janelamento Temporal de Matriz:** Acopla as defasagens preditivas chamando o método estrutural `pandas.DataFrame.shift(steps)`. `steps > 0` geram Features Lags. `steps < 0` empurram vetores abaixo isolando Feature Leads para variáveis atmosféricas e de chuva. O target é criado via `shift(-1)` (`_h1`) e `shift(-2)` (`_h2`).
- **Load (Storage Push):** Realiza Upsert do frame multi-dimensional processado integralmente pronto na tabela `ML.xtrain_horario_t_2026_V7`.

---

### Módulo B: Otimização Numérica / Modelagem (`train_lightgbm_from_bq_V7.py`)
Módulo *Stateless* onde ocorre o ajuste matemático desassociado de cargas em memórias prévias de ETL.

**Fluxo Técnico:**
- **Data Fetching:** Realiza o pull da base padronizada via Pandas GBQ.
- **Preprocessing:** Aplicação do `MinMaxScaler` em todos os vetores dinâmicos, limitando restrições de amplitude para otimizar os Gradientes Computacionais.
- **Modelagem Regressiva Multi-Classe:** Instancia o framework nativo da Microsoft: `LightGBM (LGBMRegressor)`. Diferente da aproximação em Level-Wised regular (XGBoost), o LGBM escala perfeitamente sob crescimento `leaf-wise`.
- **Framework Mutli-Output:** Conterizado através da biblioteca SciKit-Learn (`MultiOutputRegressor`), forçando alvos dinâmicos estritos multivariáveis para que o modelo calcule em um único pulso tanto o array de `_h1` quanto o array `_h2`.
- **Avaliação (Métrica Nativa):** Impressão automatizada do Mean Absolute Error (MAE) no conjunto Split-Test (Últimos 10% do DF temporal).
- **Serialização Estática:** A matriz final ajustada exporta o dump binário em `.joblib` junto de seu instanciador respectivo de Scalers multidimensionais.

---

### Módulo C: Motor de Inferência On-Demand e Walk-Forward (`main_v7.py`)
Este submódulo corresponde à subida em Produção de Machine Learning, desenhado sob micro-rotinas de 15 a 15 min de agendamento em cron job. Executa a avançada heurística de **Walk-Forward com Leads Progressivos** para alcançar 6 horizontes plenos de previsão matemática.

**Fluxo Técnico de Deploy (Laço de 6 Iterações):**
1. **Fetch Temporal Inicial:** Capta no GBQ estritas `48` horas de logs passados estabilizados para compor a Inércia Física - Lags e MAs - de partida.
2. **Fetch Predictivo Estendido Externo:** Dispara via HTTP Rest API as requisições na `OpenWeather OneCall` e `Open-Meteo`, capturando vetores climáticos das Exatas próximas 12 horas `(t+1 a t+12)`. O sobredimensionamento de clima exterior garante que o modelo nunca fique carente das features de Lead avançadas no auge de seu laço final.
3. **Pivoteamento Virtual:** Constrói Arrays concatenados de 12 horas estritas no futuro onde a "correnteza d'água" iniciante encontra-se vazia, mas as features mapeadas de Clima já suportam a medição atmosférica local.
4. **Iteração Walk-Forward:** Inicializa a predição. Ao submeter a primeira matriz (T+1), a engine extrai os Outputs Float do LightGBM (Intensidades projetadas de Correnteza). Em um movimento retrógrado intrassistema, o script **injeta de volta** essas 3 correntes preditas nos seus respectivos Arrays vazios do instante T+1.
5. **Autonomia (T+2 a T+6):** O Index recomeça deslocando virtualmente a realidade para frente (O `T0` torna-se `T+1`). A matriz recalcula as Médias de janela deslizante (Rolling MA) aceitando organicamente as variáveis recém-projetadas, misturando perfeitamente com o clima que as APIs do exterior já confirmaram que acontecerá, e descobre assim o `T+2` sucessivamente até `T+6`.
6. **Inserção Cíclica Consolidada (MERGE SQL):** Envio dos logs num único Batch de 6 timestamps na tabela de monitoramento contendo a heurística de Validação da Praticagem: (`Δt entre predição e timestamp atual situar-se entre 90 a 150min`). Evita faturamentos excedentes no Google BigQuery e resolve anomalias em dashboard.

A V7 consagra a fundição da melhor infraestrutura MLOps combinando a Estabilidade Recursiva (Walk-Forward dos lags hídricos) empurrada fortemente pelas matrizes de Forçamentos Progressivos Físicos da atmosfera adjacente (Leads climáticos reais em loco).
