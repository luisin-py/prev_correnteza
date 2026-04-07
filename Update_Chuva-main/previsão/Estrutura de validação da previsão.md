🎯 Métricas de Avaliação de Previsão
1. Accuracy (Acc)
O que é:
Percentual de previsões que ficaram “suficientemente boas”.

Regra usada:
Uma previsão é considerada certa se o erro absoluto for ≤ 0,15 metros.

Por que interessa:
Dá uma noção direta: “quantas vezes acertei no alvo?”

🔍 Analogia:
Pense num alvo de dardos com um círculo interno de 15 cm de raio.
Se o dardo cai dentro desse círculo, conta como acerto.
A Accuracy é a porcentagem de dardos dentro do círculo.

2. MAE – Mean Absolute Error (Erro Absoluto Médio)
O que é:
Média dos erros (em módulo).
Unidade: metros.

Por que interessa:
Mostra o quanto, em média, você erra – mesmo quando “acerta” segundo a regra da Accuracy.
Exemplo: um erro de 0,14 m ainda é erro!

🔍 Analogia:
No alvo, imagine medir a distância de cada dardo até o centro e tirar a média dessas distâncias.

3. σ – Desvio-padrão dos erros
O que é:
Quanta variação existe entre os erros individuais.

Interpretação:

σ pequeno → erros concentrados perto da média → desempenho consistente.

σ grande → alguns erros bem maiores ou menores que a média → desempenho instável.

🔍 Analogia:
Se todos os dardos ficam a mais ou menos a mesma distância do centro, o “espalhamento” (σ) é pequeno.

🧮 Exemplo numérico
Medição real (m)	Previsão (m)	Erro |real − prev|
1,80	1,77	0,03
2,10	2,05	0,05
1,95	2,05	0,10
1,60	1,40	0,20
2,00	2,02	0,02

Accuracy:
Limite 0,15 m: os 4 primeiros erros (0,03 a 0,10) são acertos; o de 0,20 é erro.
Accuracy = 4/5 = 80%

MAE:
(0,03 + 0,05 + 0,10 + 0,20 + 0,02) / 5 = 0,08 m

σ (desvio-padrão):
Neste exemplo ≈ 0,066 m
Ou seja, os erros ficam até ±6,6 cm em torno da média.

✅ Resumo de um resultado real
Accuracy: 99,6 % → praticamente todas as previsões ficaram dentro de 15 cm

MAE: 0,0073 m → erro médio de 7 mm (muito bom!)

σ: 0,0170 m → erros variam pouco; maioria fica até ~1,7 cm em torno do MAE

Essas três métricas juntas oferecem um retrato completo:
frequência de acertos, tamanho típico do erro e consistência do modelo de previsão.