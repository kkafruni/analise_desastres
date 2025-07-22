# Pacotes
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

# Caminhos
caminho_base <- "dados_novo_caged"
caminho_db <- "analise_desastres.db"
con <- dbConnect(duckdb::duckdb(), caminho_db)

# Colunas padrão
colunas_mov <- c(
  "competenciamov", "regiao", "uf", "municipio", "secao", "subclasse", "saldomovimentacao",
  "cbo2002ocupacao", "categoria", "graudeinstrucao", "idade", "horascontratuais", "racacor",
  "sexo", "tipoempregador", "tipoestabelecimento", "tipomovimentacao", "tipodedeficiencia",
  "indtrabintermitente", "indtrabparcial", "salario", "tamestabjan", "indicadoraprendiz",
  "origemdainformacao", "competenciadec", "indicadordeforadoprazo",
  "unidadesalariocodigo", "valorsalariofixo"
)

# Função de importação
importar_novo_caged <- function(arquivo) {
  nome_arquivo <- basename(arquivo)
  if (!str_detect(nome_arquivo, "CAGEDMOV")) return(NULL)  # Ignorar FOR e EXC
  competencia <- str_extract(nome_arquivo, "\\d{6}")
  nome_tabela <- "novo_caged_mov"

  message("📂 Lendo arquivo: ", nome_arquivo)

  df <- read_delim(
    arquivo,
    delim = ";",
    locale = locale(encoding = "Latin1"),
    col_names = colunas_mov,
    skip = 1,
    trim_ws = TRUE,
    show_col_types = FALSE
  )

  message("📑 Colunas originais:")
  print(names(df))
  message("🔍 Primeiras 10 linhas:")
  print(head(df, 10))

  df <- df %>%
    mutate(
      salario = as.numeric(gsub(",", ".", salario)),
      valorsalariofixo = as.numeric(gsub(",", ".", valorsalariofixo)),
      municipio = as.integer(municipio),
      cbo2002ocupacao = as.integer(cbo2002ocupacao),
      competencia_arquivo = competencia
    )

  if (!dbExistsTable(con, nome_tabela)) {
    message("🧱 Criando tabela ", nome_tabela)
    dbWriteTable(con, nome_tabela, df)
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_novo_caged_muni_comp ON novo_caged_mov(municipio, competencia_arquivo)")
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_novo_caged_cbo ON novo_caged_mov(cbo2002ocupacao)")
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_novo_caged_muni ON novo_caged_mov(municipio)")
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_novo_caged_comp ON novo_caged_mov(competencia_arquivo)")
  } else {
    ja_importado <- dbGetQuery(con, paste0(
      "SELECT COUNT(*) FROM ", nome_tabela,
      " WHERE competencia_arquivo = '", competencia, "'"
    ))[1,1] > 0

    if (!ja_importado) {
      message("📤 Importando dados: ", nrow(df), " linhas para ", nome_tabela)
      dbWriteTable(con, nome_tabela, df, append = TRUE)
    } else {
      message("⚠️ Competência já importada: ", competencia)
    }
  }
}

# Executar para todos os arquivos MOV
arquivos_txt <- list.files(caminho_base, pattern = "CAGEDMOV.*\\.txt$", recursive = TRUE, full.names = TRUE)
lapply(arquivos_txt, importar_novo_caged)

# Finaliza conexão
dbDisconnect(con)
cat("🏁 Novo CAGED importado com sucesso\n")
