# Buscar URLs de prefeituras e governos em GitHub

Este pacote foi preparado para rodar no GitHub Actions.

## Estrutura esperada
- `input/base_contatos.csv` -> sua base de entrada
- `output/` -> onde os resultados serão gravados
- `.github/workflows/buscar_urls_prefeituras.yml` -> workflow automático
- `run_prefeituras_urls.py` -> script principal

## Como usar no GitHub
1. Crie um repositório novo no GitHub.
2. Envie todos os arquivos desta pasta para o repositório.
3. Coloque sua planilha convertida para CSV em `input/base_contatos.csv`.
4. No GitHub, abra a aba **Actions**.
5. Execute o workflow **Buscar URLs de prefeituras e governos** em **Run workflow**.

## Resultado
Ao final, o GitHub vai gerar:
- `output/base_contatos_municipais_com_urls_v3.csv`
- `output/base_contatos_municipais_com_urls_v3.xlsx`
- `output/revisao_urls_prefeituras_v3.csv`
- `output/revisao_urls_prefeituras_v3.xlsx`

Os arquivos também ficam disponíveis como **Artifact** no workflow.

## Agendamento
O workflow já está configurado para rodar automaticamente todo dia às 03:00 UTC.
Se você não quiser agendamento, remova o bloco `schedule:` do arquivo YAML.

## Importante
- Eu não consigo deixar isso executando por conta própria a partir desta conversa.
- Mas este pacote já deixa tudo pronto para você subir no GitHub e rodar por lá sem ficar no Colab.
- Se sua base estiver em XLSX, salve como CSV antes de subir para o repositório.
