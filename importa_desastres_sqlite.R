# Pacotes
if (!require("DBI")) install.packages("DBI")
if (!require("duckdb")) install.packages("duckdb")
if (!require("readr")) install.packages("readr")
if (!require("dplyr")) install.packages("dplyr")
if (!require("fuzzyjoin")) install.packages("fuzzyjoin")
library(DBI)
library(duckdb)
library(readr)
library(dplyr)
library(fuzzyjoin)

# Função para normalizar nomes de municípios
clean_municipio <- function(x) {
  x <- toupper(iconv(x, to = "ASCII//TRANSLIT"))        # remove acentos
  x <- gsub("['`´]", "", x)                             # remove apóstrofos e crases
  x <- gsub("-", " ", x)                                # troca hífens por espaço
  x <- gsub("\\bD\\s", "DO ", x)                        # converte "D " em "DO"
  x <- gsub("\\s+", " ", x)                             # normaliza espaços duplos
  trimws(x)
}

# Caminhos
caminho_csv <- "base_desastres.csv"
caminho_municipios <- "municipios.CSV"
caminho_db <- "analise_desastres.db"
nome_tabela <- "desastres"

# Conexão
con <- dbConnect(duckdb::duckdb(), caminho_db)

# Leitura da base de desastres
mensagem_encoding <- "UTF-8"
dados_csv <- tryCatch({
  read_csv2(caminho_csv, locale = locale(encoding = "UTF-8"))
}, error = function(e) {
  mensagem_encoding <<- "Latin1"
  read_csv2(caminho_csv, locale = locale(encoding = "Latin1"))
})
cat("✓ CSV lido com sucesso com encoding:", mensagem_encoding, "\n")

# Leitura da tabela de códigos CAGED
dic_municipios <- read.csv(caminho_municipios, sep = ";", encoding = "latin1")
colnames(dic_municipios) <- c("codigo_caged", "nome_municipio_uf")

# Remove UF e normaliza nome de município
dic_municipios <- dic_municipios %>%
  mutate(
    nome_base = sub("^[^-]*-", "", nome_municipio_uf),          # remove "UF-"
    nome_municipio = clean_municipio(nome_base)
  ) %>%
  distinct(nome_municipio, .keep_all = TRUE)

cat("🧠 Após limpeza — nomes no dicionário:\n")
print(unique(head(dic_municipios$nome_municipio, 10)))

# Prepara e normaliza os nomes da base de desastres
dados_csv <- dados_csv %>%
  rename(
    cod_ibge = Cod_IBGE_Mun,
    data_desastre = Data_Evento,
    nome_municipio = Nome_Municipio
  ) %>%
  mutate(
    cod_ibge = as.integer(cod_ibge),
    data_desastre = as.character(data_desastre),
    nome_municipio = clean_municipio(nome_municipio),
    row_id = row_number()  # ID único por linha
  )

cat("🔍 Exemplos de nomes na base de desastres:\n")
print(unique(head(dados_csv$nome_municipio, 10)))

# Fuzzy join com deduplicação garantida
dados_csv <- stringdist_left_join(
  dados_csv,
  dic_municipios,
  by = "nome_municipio",
  method = "jw",
  max_dist = 0.15,
  distance_col = "distancia"
) %>%
  group_by(row_id) %>%
  slice_min(order_by = distancia, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  select(-row_id) %>%
  rename(cod_caged = codigo_caged)

# Corrige nomes para evitar .x/.y
dados_csv <- dados_csv %>%
  select(-nome_municipio.y) %>%
  rename(nome_municipio = nome_municipio.x)

# Revisar os piores matches
cat("🔎 Possíveis matches incertos (distancia > 0.1):\n")
print(
  dados_csv %>%
    filter(distancia > 0.1) %>%
    select(nome_municipio, distancia, cod_caged) %>%
    distinct() %>%
    arrange(desc(distancia)) %>%
    head(20)
)

# LOG: validações após o join
cat("✅ Total de linhas finais na base de desastres: ", nrow(dados_csv), "\n")
cat("🔗 Linhas com cod_caged preenchido: ", sum(!is.na(dados_csv$cod_caged)), "\n")

# Cria ou substitui a tabela
if (!dbExistsTable(con, nome_tabela)) {
  dbWriteTable(con, nome_tabela, dados_csv)
} else {
  dbExecute(con, paste0("DROP TABLE IF EXISTS ", nome_tabela))
  dbWriteTable(con, nome_tabela, dados_csv)
}

# Índices
message("⚙️ Criando índices...")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_desastres_codibge ON desastres(cod_ibge)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_desastres_data ON desastres(data_desastre)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_desastres_codibge_data ON desastres(cod_ibge, data_desastre)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_desastres_codcaged ON desastres(cod_caged)")

# Finaliza
dbDisconnect(con)
cat("🏁 Finalizado: desastres importados com fuzzy cod_caged e sem duplicações\n")
