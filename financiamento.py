import pandas as pd

# Parâmetros do financiamento
saldo_inicial = 280000.00
taxa_juros_mensal = 0.0066  # 0,66% ao mês
taxa_tr_mensal = 0.0017     # 0,17% ao mês (TR)
pagamento_mensal = 1991.37
amortizacao_extra_padrao = 0
amortizacao_pos18_samira = 0
amortizacao_extra_dezembro = 8000 + 1000
uso_fgts_a_cada_meses = 24
valor_fgts = 0
max_parcelas = 420

# Inicializar listas para armazenar os resultados
parcelas = []
juros = []
amortizacao_normal = []
amortizacao_total = []
pagamentos = []
saldos = []
tr_aplicada = []
fgts_utilizado = []

saldo_devedor = saldo_inicial

for parcela in range(max_parcelas):
    if saldo_devedor <= 0:
        break

    parcelas.append(parcela)

    if parcela == 0:
        juros.append(0)
        amortizacao_normal.append(0)
        amortizacao_total.append(0)
        pagamentos.append(0)
        saldos.append(saldo_devedor)
        tr_aplicada.append(0)
        fgts_utilizado.append(0)
        continue

    # Juros do mês
    juro_mes = saldo_devedor * taxa_juros_mensal
    amortizacao = pagamento_mensal - juro_mes


    # Amortização extra
    amortizacao_extra = amortizacao_extra_padrao
    if parcela % 12 == 11:
        amortizacao_extra += amortizacao_extra_dezembro
    if parcela >= 24:
        amortizacao_extra += amortizacao_pos18_samira
    if parcela == 18:
        amortizacao_extra += 20000

    amortizacao_final = amortizacao + amortizacao_extra
    saldo_devedor -= amortizacao_final

    # TR aplicada após amortização
    tr_mes = saldo_devedor * taxa_tr_mensal
    saldo_devedor += tr_mes

    # FGTS a cada 24 meses (exceto na parcela 0)
    fgts_mes = 0
    if parcela % uso_fgts_a_cada_meses == 0:
        fgts_mes = min(valor_fgts, saldo_devedor)
        saldo_devedor -= fgts_mes

    # Corrige se o saldo devedor ficar negativo
    if saldo_devedor < 0:
        saldo_devedor = 0

    # Armazenar dados no DataFrame
    juros.append(round(juro_mes, 2))
    amortizacao_normal.append(round(amortizacao, 2))
    amortizacao_total.append(round(amortizacao_final, 2))
    pagamentos.append(pagamento_mensal)
    tr_aplicada.append(round(tr_mes, 2))
    fgts_utilizado.append(round(fgts_mes, 2))
    saldos.append(round(saldo_devedor, 2))

# Criar DataFrame
df = pd.DataFrame({
    "Parcela": parcelas,
    "Juros": juros,
    "Amortizacao_Normal": amortizacao_normal,
    "Amortizacao_Total": amortizacao_total,
    "Pagamento": pagamentos,
    "TR_Mensal": tr_aplicada,
    "FGTS_Utilizado": fgts_utilizado,
    "Saldo_Devedor": saldos
})

# Salvar em CSV
caminho_arquivo = "/home/negan/Downloads/simulacao_com_fgts_tr.csv"
df.to_csv(caminho_arquivo, index=False)
