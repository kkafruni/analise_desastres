# Pacotes
if (!require("DBI")) install.packages("DBI")
if (!require("duckdb")) install.packages("duckdb")
if (!require("readr")) install.packages("readr")
if (!require("dplyr")) install.packages("dplyr")
if (!require("tidyr")) install.packages("tidyr")
if (!require("did")) install.packages("did")
if (!require("progressr")) install.packages("progressr")

library(DBI)
library(duckdb)
library(readr)
library(dplyr)
library(tidyr)
library(did)
library(progressr)

# Normalização
clean_municipio <- function(x) {
  x <- toupper(iconv(x, to = "ASCII//TRANSLIT"))
  x <- gsub("['`´]", "", x)
  x <- gsub("-", " ", x)
  x <- gsub("\\bD\\s", "DO ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

# Caminhos
caminho_db <- "analise_desastres.db"
caminho_dic_municipios <- "municipios.CSV"
saida_csv <- "resultado_desastres_hidrologicos_vs_admissoes.csv"

# Conexão
con <- dbConnect(duckdb::duckdb(), caminho_db)

# Dicionário de municípios
dic_municipios <- read.csv(caminho_dic_municipios, sep = ";", encoding = "latin1") %>%
  rename(codigo_caged = Código, nome_municipio_uf = Descrição) %>%
  mutate(nome_municipio = clean_municipio(sub("^[^-]*-", "", nome_municipio_uf))) %>%
  distinct(codigo_caged, nome_municipio)

# Query otimizada
query <- "
WITH desastres_filtrados AS (
  SELECT *
  FROM main.desastres
  WHERE descricao_tipologia IN (
    'Inundações', 'Alagamentos', 'Chuvas Intensas', 'Enxurradas', 'Erosão',
    'Estiagem e Seca'
  )
  AND (DH_total_danos_humanos >= 100 OR DH_MORTOS >= 10)
),
desastres_hidrologicos AS (
  SELECT
    cod_caged,
    STRFTIME(STRPTIME(data_desastre, '%d/%m/%Y'), '%Y') AS ano,
    STRFTIME(STRPTIME(data_desastre, '%d/%m/%Y'), '%m') AS mes,
    descricao_tipologia
  FROM desastres_filtrados
),
impactos_humanos AS (
  SELECT
    cod_caged,
    STRFTIME(STRPTIME(data_desastre, '%d/%m/%Y'), '%Y') AS ano,
    STRFTIME(STRPTIME(data_desastre, '%d/%m/%Y'), '%m') AS mes,
    SUM(DH_total_danos_humanos) AS total_danos_humanos,
    SUM(DH_MORTOS) AS total_mortos
  FROM desastres_filtrados
  GROUP BY cod_caged, STRFTIME(STRPTIME(data_desastre, '%d/%m/%Y'), '%Y'), STRFTIME(STRPTIME(data_desastre, '%d/%m/%Y'), '%m')
),
caged_base AS (
  SELECT 
    municipio,
    competencia_arquivo,
    saldomovimentacao AS saldo,
    cbo2002ocupacao
  FROM main.novo_caged_mov
  UNION ALL
  SELECT 
    municipio,
    competencia_arquivo,
    saldo_mov AS saldo,
    cbo_2002_ocupacao
  FROM main.caged_antigo
),
caged_completo AS (
  SELECT 
    c.municipio,
    STRFTIME(STRPTIME(c.competencia_arquivo, '%Y%m'), '%Y') AS ano,
    STRFTIME(STRPTIME(c.competencia_arquivo, '%Y%m'), '%m') AS mes,
    c.saldo,
    q.nivel_conhecimento,
    q.nivel_habilidade,
    q.nivel_atitude,
    q.nivel_ocupacao
  FROM caged_base c
  LEFT JOIN main.qbq q ON c.cbo2002ocupacao = q.cbo2002ocupacao
)
SELECT 
  c.municipio AS codigo_caged,
  c.ano,
  c.mes,
  COUNT(DISTINCT CASE WHEN d.descricao_tipologia = 'Inundações' THEN 1 END) AS Inundacoes,
  COUNT(DISTINCT CASE WHEN d.descricao_tipologia = 'Alagamentos' THEN 1 END) AS Alagamentos,
  COUNT(DISTINCT CASE WHEN d.descricao_tipologia = 'Chuvas Intensas' THEN 1 END) AS Chuvas_Intensas,
  COUNT(DISTINCT CASE WHEN d.descricao_tipologia = 'Enxurradas' THEN 1 END) AS Enxurradas,
  COUNT(DISTINCT CASE WHEN d.descricao_tipologia = 'Erosão' THEN 1 END) AS Erosao,
  COUNT(DISTINCT CASE WHEN d.descricao_tipologia = 'Estiagem e Seca' THEN 1 END) AS Estiagem_e_Seca,
  SUM(CASE WHEN c.saldo = 1 THEN 1 ELSE 0 END) AS total_admissoes,
  AVG(c.nivel_conhecimento) AS media_nivel_conhecimento,
  AVG(c.nivel_habilidade) AS media_nivel_habilidade,
  AVG(c.nivel_atitude) AS media_nivel_atitude,
  AVG(c.nivel_ocupacao) AS media_nivel_ocupacao,
  ih.total_danos_humanos AS DH_total_danos_humanos,
  ih.total_mortos AS DH_MORTOS
FROM caged_completo c
LEFT JOIN desastres_hidrologicos d 
  ON c.municipio = d.cod_caged AND c.ano = d.ano AND c.mes = d.mes
LEFT JOIN impactos_humanos ih
  ON c.municipio = ih.cod_caged AND c.ano = ih.ano AND c.mes = ih.mes
GROUP BY c.municipio, c.ano, c.mes, ih.total_danos_humanos, ih.total_mortos
"


# Caminho para cache
caminho_cache_query <- "cache_resultado_query.rds"

# Consulta com cache
if (file.exists(caminho_cache_query)) {
  cat("⚡️ Carregando resultado_query do cache...\n")
  resultado_query <- readRDS(caminho_cache_query)
} else {
  cat("⏳ Executando consulta no banco de dados...\n")
  resultado_query <- dbGetQuery(con, query)
  saveRDS(resultado_query, caminho_cache_query)
  cat("💾 Resultado da query salvo em cache para uso futuro.\n")
}

# Garante tipos corretos
resultado_query <- resultado_query %>%
  mutate(
    ano = as.integer(ano),
    mes = as.integer(mes),
    competencia = ano * 100 + mes
  )

# Identifica o primeiro desastre por município
primeiro_desastre <- resultado_query %>%
  filter(
    Inundacoes > 0 | Alagamentos > 0 | Chuvas_Intensas > 0 |
    Enxurradas > 0 | Erosao > 0 | Estiagem_e_Seca > 0
  ) %>%
  group_by(codigo_caged) %>%
  summarise(competencia_primeiro_desastre = min(competencia), .groups = "drop")

# Junta ao resultado principal
resultado_query <- resultado_query %>%
  left_join(primeiro_desastre, by = "codigo_caged") %>%
  mutate(
    treat = if_else(!is.na(competencia_primeiro_desastre), 1, 0),
    post = if_else(
      !is.na(competencia_primeiro_desastre) & competencia >= competencia_primeiro_desastre,
      1, 0
    )
  )


library(purrr)

# Divide a base por ano
splits_por_ano <- split(resultado_query, resultado_query$ano)

# Garante pasta de saída
dir.create("resultados_divididos", showWarnings = FALSE)

# Para cada ano, processa e salva
walk(names(splits_por_ano), function(ano_atual) {
  cat("⏳ Processando ano:", ano_atual, "\n")
  
  dados_ano <- splits_por_ano[[ano_atual]] %>%
    mutate(
      codigo_caged = as.integer(codigo_caged),
      ano = as.integer(ano),
      mes = as.integer(mes)
    )
  
  # Recupera meses desse ano
  meses_ano <- unique(dados_ano$mes)
  
  # Municípios com esse ano no dicionário
  municipios_ano <- dic_municipios %>%
    filter(codigo_caged %in% dados_ano$codigo_caged)
  
  municipios_meses <- expand.grid(
    codigo_caged = municipios_ano$codigo_caged,
    ano = ano_atual,
    mes = meses_ano,
    stringsAsFactors = FALSE
  ) %>%
    mutate(
      codigo_caged = as.integer(codigo_caged),
      ano = as.integer(ano),
      mes = as.integer(mes)
    )
  
  resultado_final_ano <- municipios_meses %>%
    left_join(dados_ano, by = c("codigo_caged", "ano", "mes")) %>%
    left_join(dic_municipios, by = "codigo_caged") %>%
    mutate(across(
      c(Inundacoes, Alagamentos, Chuvas_Intensas, Enxurradas, Erosao, Estiagem_e_Seca,
        total_admissoes, media_nivel_conhecimento, media_nivel_habilidade,
        media_nivel_atitude, media_nivel_ocupacao,
        DH_total_danos_humanos, DH_MORTOS),
      ~replace_na(.x, 0)
    )) %>%
    select(
      codigo_caged,
      nome_municipio,
      ano,
      mes,
      everything()
    )
  
  # Exporta por ano
  write_csv(resultado_final_ano, paste0("resultados_divididos/resultado_", ano_atual, ".csv"))
  cat("✅ Ano", ano_atual, "exportado com sucesso.\n")
})


# Junta tudo (opcional)
arquivos <- list.files("resultados_divididos", full.names = TRUE)
resultado_geral <- purrr::map_dfr(arquivos, read_csv)
write_csv(resultado_geral, saida_csv)

# Finaliza
dbDisconnect(con, shutdown = TRUE)
cat("🏁 Resultado final exportado para:", saida_csv, "\n")

# Limpa NA da variável de desfecho
dados_did <- resultado_geral %>%
  select(codigo_caged, competencia, competencia_primeiro_desastre, treat, total_admissoes) %>%
  drop_na(total_admissoes) %>%
  mutate(
    g = if_else(treat == 1, competencia_primeiro_desastre, 0)
  ) %>%
  select(codigo_caged, competencia, g, total_admissoes)


handlers(global = TRUE)
handlers("txtprogressbar")  # você pode usar "progress" ou "rstudio" se preferir

message("🚀 Iniciando Callaway & Sant’Anna...")

with_progress({
  did_result <- att_gt(
    yname = "total_admissoes",
    tname = "competencia",
    idname = "codigo_caged",
    gname = "g",
    data = dados_did,
    panel = FALSE,
    control_group = "nevertreated"
  )
})


# Agregação por grupo tratado
agg_group <- aggte(did_result, type = "group")
print(summary(agg_group))

# Efeitos dinâmicos (lead/lag)
agg_dyn <- aggte(did_result, type = "dynamic")
print(summary(agg_dyn))
plot(agg_dyn)