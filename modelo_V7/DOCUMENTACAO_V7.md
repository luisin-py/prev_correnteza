# 📘 Arquitetura e Engenharia da Versão 7 (V7) - Previsão de Correnteza

Este documento detalha o paradigma implementado na **V7**, que revoluciona o motor preditivo do Porto de Rio Grande (Praticagem) introduzindo a mecânica de "Leads Meteorológicos".

---

## 1. O Problema Histórico (Antes da V7)
Nos modelos anteriores (V4 a V6), o algoritmo utilizava a estratégia de *Walk-Forward Recursiva* pura baseada na **inércia do sistema**.
Isso significava que para prever a correnteza daqui a `+4H`, o modelo calculava a `+1H`, atualizava o passado da água com a própria previsão, e deduzia a próxima hora repetidamente. A chuva e o vento eram previstos cegamente na estrutura temporal, com uma dependência enorme da inércia prévia.

## 2. A Solução V7: "A Arte de Antecipar o Clima" (Leads)
Na Versão 7, em vez de depender estritamente dos "Lags" (o que a água fazia no passado), ensinamos o Scikit-Learn/LightGBM a interpretar o Clima do Futuro simultaneamente no instante `T0`.

Introduzimos a regra de **Leads** (horizontes preditivos deslocados no dataframe `pandas.shift(-L)`), onde para cada momento da Baía, o modelo "espia" no horizonte o **Vento, Chuva, Pressão e Umidade** das horas `+1H`, `+2H`, `+3H`, `+4H` e `+5H`. 

### Vantagem Oculta
O modelo descobre dinamicamente fatores como: *"Se no momento a água está parada, mas daqui a 3 horas a pressão cair bruscamente com a chegada de rajadas de chuva a 30 nós de Sul, a Correnteza da Superfície reverterá para +1.5Kt imediatamente"*.

---

## 3. Escopo de Scripts na V7

Com o intuito de garantir as boas práticas de Engenharia de Dados MLOps, a rotina foi segmentada em 3 grandes cérebros de ação:

### A) `build_features_V7.py` (O Fabricante de Base)
Responsável EXCLUSIVAMENTE por criar o DataSet limpo.
- Captura via requisição HTTP de hora em hora o volume Pluviométrico de 9 Bacias do RS (pela Open-Meteo Historic Archive) desde o ano 2020.
- Executa a Bússola Setorial (Dividindo de 0-360 Graus para os 16 vetores perfeitos de `22.5º` da Hidrodinâmica local).
- Agrega em Lags (`_lagX`), Mães Móveis (`_maX`) e Leads da meteorologia do futuro (`_leadX`).
- **Geração Mestra:** Salva na Nuvem Google (BigQuery) na Tabela Oficial Fria: `local-bliss-359814.ML.xtrain_horario_t_2026_V7`.

### B) `train_lightgbm_from_bq_V7.py` (A Máquina de Treino)
Ao invés de carregar o CSV na memória pesadamente gerando retrabalhos nulos, este código atua nativamente em nuvem.
- Baixa o DataSet fabricado pelo Step 1 puramente (`ML.xtrain_horario_t_2026_V7`).
- Executa a lógica temporal (80/10/10 Validação/Teste).
- Roda o Motor Assíncrono `MultiOutputRegressor(LGBMRegressor)` da Microsoft/LightGBM. O modelo atinge a impressionante marca de `0.29 Kt` Absolutos de Erro Mediano no target de correntes à frente.
- Cospe em disco o formato congelado universal `modelo_LightGBM.joblib` + Scalers.

### C) `main_v7.py` (A Inferência Oficial de Produção)
O Código-Vivo que roda com os Schedulers de 15 minutos em Nuvem / Container local.
1. Carrega as últimas 48H da Vida Real da Boia (Sensor da Barra/Canal).
2. Toca na API `OpenWeather OneCall 3.0` da chave resgatada secretamente para as 5 horas do futuro imediato.
3. Ativa a lógica da **"Grade Fantasma"** (Phantom rows): Linhas futuras com a Meteorologia verdadeira fornecida pela API, mas a previsão de correnteza trancada como `NaN`.
4. Ao empilhar, aplicar `shift` retrospectivo, o Target Base `T0` (O Agora), capta o clima inteiro em uma tacada só e chuta a predição para `+1H` e `+2H`.
5. Dá o **Upsert (MERGE)** na `previsoes_oficiais`. Se for a regra do "Segundo Tempo" (Diferença entre o TGT de 90 a 150min), preenche com sucesso o `primeiro_calculo` da Praticagem.

---

> Desenhada para robustez máxima na Previsão da Praticagem de Rio Grande sob Tempestades Ciclônicas de Baixa Pressão que chegam na Barra sem aviso da Água antes do Vento!
