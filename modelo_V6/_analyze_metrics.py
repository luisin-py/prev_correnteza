import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd

df = pd.read_csv(r'c:\Users\LUIS\Desktop\previsoes_dados_agua\modelo_V5\metricas_recursivas_v5.csv')
sup_cols = [c for c in df.columns if 'superficie' in c]

print('=== MEDIA SUPERFICIE por modelo (todos horizontes) ===')
r2_cols = [c for c in sup_cols if 'R2' in c]
acc_cols = [c for c in sup_cols if 'Acc05' in c]
mae_cols = [c for c in sup_cols if 'MAE' in c]

for m in df['Model'].unique():
    sub = df[df['Model'] == m]
    avg_r2 = sub[r2_cols].mean().mean()
    avg_acc = sub[acc_cols].mean().mean()
    avg_mae = sub[mae_cols].mean().mean()
    print(f'{m:25s} R2={avg_r2:.3f}  Acc05={avg_acc:.2f}%  MAE={avg_mae:.3f}')
