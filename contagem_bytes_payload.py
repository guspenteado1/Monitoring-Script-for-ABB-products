def formatar_em_pares(texto):
    pares = [texto[i:i+2] for i in range(0, len(texto), 2)]
    return ' '.join(pares), len(pares)
 
entrada = input("Digite o payload (sem espaços): ")
 
if len(entrada) % 2 != 0:
    print("Erro: O payload precisa ter um número par de caracteres.")
else:
    saida_formatada, quantidade_pares = formatar_em_pares(entrada)
    print("\nPayload:")
    print(saida_formatada)
    print(f"\nQuantidade de bytes: {quantidade_pares}")