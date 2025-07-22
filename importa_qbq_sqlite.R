# Carregar pacotes necessários
if (!require("DBI")) install.packages("DBI")
if (!require("duckdb")) install.packages("duckdb")
if (!require("readr")) install.packages("readr")
if (!require("stringr")) install.packages("stringr")
if (!require("dplyr")) install.packages("dplyr")
library(DBI)
library(duckdb)
library(readr)
library(stringr)
library(dplyr)

# Conexão com o banco de dados
con <- dbConnect(duckdb::duckdb(), "analise_desastres.db")

# Caminho para o arquivo CSV
arquivo_qbq <- "qbq.csv"

# Leitura do CSV com codificação e separador corretos
df_qbq <- read_csv(arquivo_qbq, locale = locale(encoding = "UTF-8"))

# Renomear colunas para snake_case padronizado
df_qbq <- df_qbq %>%
  rename_with(~ str_replace_all(., c(
    "CodCBO" = "cbo2002ocupacao",
    "Ocupação" = "ocupacao",
    "Síntese" = "sintese",
    "PerfilOcupacional" = "perfil_ocupacional",
    "NivelConhecimento" = "nivel_conhecimento",
    "NivelHabilidade" = "nivel_habilidade",
    "NivelAtitude" = "nivel_atitude",
    "NivelOcupacao" = "nivel_ocupacao"
  )))

# Conversão explícita das colunas de nível para INTEGER
df_qbq <- df_qbq %>%
  mutate(
    cbo2002ocupacao    = as.integer(cbo2002ocupacao),
    nivel_conhecimento = as.integer(nivel_conhecimento),
    nivel_habilidade   = as.integer(nivel_habilidade),
    nivel_atitude      = as.integer(nivel_atitude),
    nivel_ocupacao     = as.integer(nivel_ocupacao)
  )

# Visualização das colunas após renomeação e conversão
print(str(df_qbq))

# Criação da tabela no banco (sobrescrevendo)
dbWriteTable(con, "qbq", df_qbq, overwrite = TRUE)

# Índice para facilitar cruzamentos com CAGED
if ("cbo2002ocupacao" %in% names(df_qbq)) {
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_qbq_cbo ON qbq(cbo2002ocupacao)")
}

# Encerrar a conexão
dbDisconnect(con)

cat("✅ Tabela 'qbq' importada com sucesso com tipos ajustados para INTEGER\n")
