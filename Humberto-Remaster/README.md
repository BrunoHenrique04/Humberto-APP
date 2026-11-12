# Humberto Remaster

Global Solution de Cloud Database · Big Data & Analytics

O Humberto cruza estações da rede sobre trilhos de São Paulo com dados do entorno do Censo 2022. A camada Gold apresenta indicadores territoriais para triagem de vistorias. O app compara alternativas no piloto Luz ↔ Paraíso e exibe os dados disponíveis em cada estação.

## Demonstração rápida

Requisitos: .NET SDK 8, Node 20.19+ ou 22.12+ e npm. O snapshot em `evidence/gold-snapshot.json` contém resultados reais de uma execução local do pipeline.

```bash
(cd app && npm ci && npm run build)
mkdir -p api/wwwroot
cp -R app/dist/. api/wwwroot/
(cd api && GoldSnapshot=../evidence/gold-snapshot.json dotnet run --urls http://127.0.0.1:5180)
```

Abra [http://127.0.0.1:5180](http://127.0.0.1:5180), selecione o sentido do percurso e clique em **Comparar**. Para desenvolvimento da interface, use `npm run dev` em `app/` com a API na porta 5180.

Endpoints disponíveis:

| Método | Caminho | Resultado |
| --- | --- | --- |
| GET | `/api/health` | Versão da Gold |
| GET | `/api/stations` e `/api/stations/{id}` | Indicadores por registro estação–linha |
| GET | `/api/lines` e `/api/lines/{id}` | Indicadores por linha |
| GET | `/api/pilot` | Origem e destino atendidos |
| POST | `/api/routes/compare` | Alternativas e estações do piloto |

Exemplo:

```bash
curl -fsS http://127.0.0.1:5180/api/routes/compare \
  -H 'Content-Type: application/json' \
  -d '{"origin":"Luz","destination":"Paraíso","preference":"fewer_transfers"}'
```

As alternativas foram cadastradas conforme a rede oficial para o piloto. As duas têm seis paradas após a partida. A opção Azul é direta, e a opção Amarela–Verde tem uma integração. O comparador aceita ambos os sentidos. O produto ainda precisa de um grafo de rede para aceitar outros pares e de evidência direta para recomendar trajetos por acessibilidade.

## Executar o pipeline

Requisitos: Python 3.11 ou 3.12, Java 17 ou 21, acesso aos portais públicos e cerca de 2 GB livres para ambiente, arquivos de entrada e saídas.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r pipeline/requirements.txt
python pipeline/sources.py
python pipeline/build.py
python pipeline/benchmark.py
```

- **Bronze** em `data/bronze/` recebe os arquivos oficiais. As URLs, tamanhos e hashes SHA-256 constam do manifesto incorporado ao snapshot.
- **Silver** em `data/silver/` contém os setores normalizados em Parquet e as associações espaciais estação–setor. O pré-processamento geométrico usa GeoPandas em EPSG:31983. O Spark realiza leitura, validação de chaves, junções e agregações.
- **Gold** em `data/gold/` contém Parquet por estação e linha, mais o snapshot JSON. A API lê uma versão do snapshot na inicialização. Para usar a execução mais recente, inicie a API no diretório `api/` com `GoldSnapshot=../data/gold/snapshot.json`.
- **Evidências** em `evidence/` incluem o snapshot e as medidas do benchmark local. Cada nova execução gera seu próprio log Spark em `data/spark-events/`.

O catálogo do IBGE publica dados de moradores em faces com calçadas, iluminação e calçadas sem obstáculos por setor censitário. A Gold agrega os percentuais publicados com pesos iguais à área de cada setor interceptada pelo buffer de 400 m. O indicador descreve o entorno. A vistoria de uma rota exige evidências para seus trechos, travessias e acessos.

## Databricks

Importe `pipeline/databricks_experiment.py` como notebook. Copie `data/silver/sectors/` e `data/silver/station_sectors.jsonl` para um Volume. Preencha os widgets `silver` e `output`, rode as células e confira os resultados contra `evidence/gold-snapshot.json`. Registre a configuração do compute, o plano físico e os detalhes da execução na Spark UI. A execução registrada aqui usa Spark local com duas threads. O notebook está preparado para execução no Databricks.

## Fontes

- [Novo GeoSampa, visualização indicada para conferência](https://novogeosampa.prefeitura.sp.gov.br/#map=18.15/-23.62146/-46.62761/0&layers=103/100/100/&basemap=5/100/100)
- [Tutorial do GeoSampa e serviço WFS oficial](https://geoinfo-smdu.github.io/tutorial-geosampa-internet-2.0/)
- [IBGE, características urbanísticas do entorno](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/tipologias-do-territorio/24702-caracteristicas-urbanisticas-do-entorno-dos-domicilios.html)
- [Metrô de São Paulo, integração Consolação–Paulista](https://www.metro.sp.gov.br/sua-viagem/linhas-estacoes/linha-2-verde/estacao-consolacao/)
- [Humberto original, de 2024](https://github.com/BrunoHenrique04/Humberto-APP)
