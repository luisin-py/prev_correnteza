# 📘 Livro de Arquitetura: Pipeline de Predição de Correnteza (V7)

Este documento descreve detalhadamente o fluxo de engenharia, processamento de dados e inferência de ponta a ponta do projeto de **Previsão de Correnteza e Dinâmica Portuária de Rio Grande**. O modelo foi desenhado para observar o comportamento físico do canal e antecipar a turbulência da maré utilizando Inteligência Artificial de alta performance (LightGBM).

---

## 1. O Conceito Físico do Modelo
Para prever a força da correnteza em alto mar, o modelo baseia-se em três pilares fundamentais no eixo do tempo:

1. **A Inércia Física (LAGs e Médias Móveis):** O oceano não para abruptamente. O modelo analisa as últimas **1 a 5 horas no passado** da própria água e do clima (lags) e médias (MAs) para entender o momento absoluto do canal.
2. **A Força do Ambiente Atual (T0):** Nível estático, temperatura local, umidade e posição do sensor.
3. **O Ímã Climático do Futuro (LEADs):** A grande inovação. Sensores de água reagem ao que a atmosfera *fará*. O modelo recebe antecipadamente as **previsões atmosféricas (Vento, Temperatura, Chva) de 1 a 5 horas no futuro**, garantindo preparo antecipado para ciclones extratropicais ou ressacas repentinas.

---

## 2. A Trindade de Execução
Para garantir redundância, segurança em nuvem e velocidade, o sistema é arquiteturalmente dividido em três Códigos Python (Módulos Independentes):

### 🛠️ Módulo 1: A Fábrica de Treinamento (`build_features_V7.py`)
**Objetivo:** Agrupar vastos anos de dados reais para construir a tabela mestre onde a Inteligência Artificial vai estudar e aprender a dinâmica do mar.

**Passo a Passo de como funciona:**
- **Coleta Externa (Open-Meteo):** Realiza chamadas a APIs meteorológicas históricas, baixando o índice pluviométrico (chuva) hora a hora absoluto de 9 grandes Bacias Hidrográficas do Sul do Brasil.
- **Merge da Boia (BigQuery):** Acopla o volume de água das chuvas ao gigantesco histórico da Boia Praticagem armazenado (Mais de 40 mil horas cruzadas de vento, umidade, e correnteza em 3 camadas de profundidade diferentes).
- **Tratamento de Bússola:** A bússola real navega em `360 graus`. Isso confunde a Inteligência Artificial, gerando ruído linear. O script "fatia" toda coordenada de vento em **16 Quadrantes Geométricos de 22.5 Graus** de varredura.
- **Tratamento Vetorial Absoluto:** A água entrando ou saindo ("Enchente e Vazante") foi isolada das métricas, fornecendo à matemática do modelo o vetor de velocidade nua e crua para que ele mensure apenas a "Força Absoluta" desconsiderando polos negativos e positivos matemáticos.
- **Teia Temporal:** A partir desse purê de informações, gera matrizes de *Lags* (deslocando a tabela linhas para baixo, simulando o passado), e matrizes de *Leads* (Puxando matrizes do Clima para cima). 
- **Destino:** A base 100% pronta com mais de 250 colunas, sem formatações obscuras, é enviada em definitivo para o `BigQuery` na nuvem (`ML.xtrain_horario_t_2026_V7`). Nenhum script seguinte precisa fazer processamento pesado nunca mais.

---

### 🧠 Módulo 2: O Cérebro Matemático (`train_lightgbm_from_bq_V7.py`)
**Objetivo:** Este módulo foi construído inteiramente com propósito de **treinamento assíncrono isolado**. Isso nos permite re-treinar a IA na nuvem ou em máquinas de baixo porte, já que ela só precisa baixar e engolir os números mastigados.

**Passo a Passo de como funciona:**
- **Download Cru:** Faz um Select simples na matriz mestre gerada pelo passo anterior e aplica um Scaler (Normalização entre `0` e `1`) em todos os vetores numéricos para prevenir o viés estatístico de grandezas mistas (ex: Pressão 1012 VS Correnteza 0.8).
- **Engine Opt-In:** Carrega a malha `MultiOutputRegressor` encapsulando as Árvores Baseadas Em Histograma do **LightGBM** (Otimizadas pela Microsoft). O sistema entende que precisa chutar alvos diretos em matrizes MultiClasse (`Correnteza +1H` e `Correnteza +2H`).
- **Validação de Sangue Frio:** Avalia cegamente o teste gerando um log rápido do erro Absoluto Médio nas predições (MAE) contra cenários que ele nunca tinha visto na vida.
- **Persistência:** O algoritmo treinado salva a si mesmo como arquivos curtos gerados em `.joblib` junto com seus Scalers de redimensionamento na sua respectiva pasta raiz, prontos para uso infinito.

---

### 🟢 Módulo 3: O Operário (Inferência ao Vivo) - `main_v7.py`
**Objetivo:** Rodar silenciosamente de 15 em 15 minutos em Produção para proteger a manobrabilidade dos navios do porto, dando as ordens para os paineis visuais dos práticos.

**Como o Robô pensa na Produção:**
1. **Verificação do Acervo (O T0):** O script interroga o Banco de Dados requisitando exatamente o que ocorreu nas últimas 48 horas reais registradas pelos Sensores.
2. **A "Grade Fantasma" (The Phantom Grid):** Este é o pulo do gato. Como o modelo foi treinado enxergando "5 horas do Futuro Climático" (`_leadX`), o robô invoca a API moderna (`OneCall 3.0 OpenWeather`) e da (`Open-Meteo`) para resgatar como será a pressão, vento e chuva das próximas 5 horas na barra. O script as agrupa dentro de um Grid provisório onde a "Correnteza do Futuro" está forçadamente sinalizada como Cega (`NaN`).
3. **Escorregamento Automático:** Ao aplicar a lógica linear idêntica do treinamento nas matrizes agrupadas, a Mágica Natural do Pandas acontece. Os blocos do futuro preenchem retroativamente a Janela do Tempo Atuali (`T0`), servindo os ventos que acontecerão num prato limpo sem depender das pesadas chaves for-loop de repetição recursiva computacional.
4. **Alimentação Cega:** A matriz contendo "Inércia Recente + Atmósfera Futura Certa" bate na porta do LightGBM previamente treinado, pedindo socorro, e o modelo entrega de bate pronto os Valores Estáticos da Pressão Oceânica para o T+1 e T+2.
5. **Insert Estratégico (Regra das 2h):** Os números sobem ordenados sob protocolo de `MERGE` oficial na base da nuvem. O Script sabe que só deve selar (congelar em pedra firme) o número oficial das 2 Horas se o lapso temporal entre "A Geração do Dado via IA" real contra "O Horário Alvo Oficial" bater exatos entre 90 a 150 Minutos. Assim gerando as estritas Views Limpas de Veracidade.

Tudo coexiste sem fricção mantendo as correntes hídricas de Rio Grande preditivas de forma matematicamente infalível.
