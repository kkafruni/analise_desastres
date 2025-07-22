# Pacotes
if (!require("DBI")) install.packages("DBI")
if (!require("duckdb")) install.packages("duckdb")
if (!require("data.table")) install.packages("data.table")
if (!require("stringr")) install.packages("stringr")
if (!require("dplyr")) install.packages("dplyr")
if (!require("stringi")) install.packages("stringi")
library(DBI)
library(duckdb)
library(data.table)
library(stringr)
library(dplyr)
library(stringi)

# Conexão
caminho_base <- "dados_caged_antigo"
caminho_db <- "analise_desastres.db"
con <- dbConnect(duckdb::duckdb(), caminho_db)

# Importações anteriores
competencias_importadas <- character(0)
if (dbExistsTable(con, "caged_antigo")) {
  competencias_importadas <- dbGetQuery(con, "SELECT DISTINCT competencia_arquivo FROM caged_antigo")$competencia_arquivo
}

# Função de importação
importar_caged_antigo <- function(arquivo) {
  nome_arquivo <- basename(arquivo)
  competencia <- str_extract(nome_arquivo, "\\d{6}")
  competencia <- paste0(substr(competencia, 3, 6), substr(competencia, 1, 2))  # MMYYYY → YYYYMM
  if (is.na(competencia) || competencia %in% competencias_importadas) return(NULL)

  message("📂 Lendo arquivo: ", nome_arquivo)
  t_inicio_leitura <- Sys.time()

  df <- fread(arquivo, sep = ";", encoding = "Latin-1", dec = ",", fill = TRUE, showProgress = TRUE)

  t_fim_leitura <- Sys.time()
  message("⏱ Tempo de leitura: ", round(difftime(t_fim_leitura, t_inicio_leitura, units = "mins"), 2), " minutos")

  # Padronizar nomes das colunas para snake_case minúsculo
  names(df) <- names(df) |>
    stri_trans_general("Latin-ASCII") |>
    gsub("[^A-Za-z0-9]", "_", x = _) |>
    gsub("_+", "_", x = _) |>
    gsub("^_|_$", "", x = _) |>
    tolower()

  # Renomear e ajustar colunas específicas
  df <- sanitize_columns(df, competencia)

  t_inicio_escrita <- Sys.time()
  message("📤 Gravando dados no banco: ", nrow(df), " linhas")

  if (!dbExistsTable(con, "caged_antigo")) {
    message("🧱 Criando estrutura completa da tabela caged_antigo...")
    dbExecute(con, "
    CREATE TABLE caged_antigo (
    admitidos_desligados INTEGER,
    competencia_declarada TEXT,
    competencia_arquivo TEXT,
    municipio INTEGER,
    ano_declarado INTEGER,
    cbo_2002_ocupacao INTEGER,
    cnae_1_0_classe TEXT,
    cnae_2_0_classe INTEGER,
    cnae_2_0_subclas TEXT,
    faixa_empr_inicio_jan INTEGER,
    grau_instrucao INTEGER,
    qtd_hora_contrat INTEGER,
    ibge_subsetor INTEGER,
    idade INTEGER,
    ind_aprendiz INTEGER,
    ind_portador_defic INTEGER,
    raca_cor INTEGER,
    salario_mensal REAL,
    saldo_mov INTEGER,
    sexo INTEGER,
    tempo_emprego INTEGER,
    tipo_estab INTEGER,
    tipo_defic TEXT,
    tipo_mov_desagregado INTEGER,
    uf INTEGER,
    bairros_sp INTEGER,
    bairros_fortaleza INTEGER,
    bairros_rj INTEGER,
    distritos_sp INTEGER,
    regioes_adm_df INTEGER,
    mesorregiao INTEGER,
    microrregiao INTEGER,
    regiao_adm_rj TEXT,
    regiao_adm_sp INTEGER,
    regiao_corede TEXT,
    regiao_corede_04 INTEGER,
    regiao_gov_sp INTEGER,
    regiao_senac_pr INTEGER,
    regiao_senai_pr TEXT,
    regiao_senai_sp INTEGER,
    sub_regiao_senai_pr INTEGER,
    ind_trab_parcial INTEGER,
    ind_trab_intermitente INTEGER
  )")
  }

  # Escrita em chunks com transação manual
  splits <- split(df, ceiling(seq_len(nrow(df)) / 50000))
  dbBegin(con)
  for (i in seq_along(splits)) {
    message("🧩 Gravando chunk ", i, " de ", length(splits))
    dbWriteTable(con, "caged_antigo", splits[[i]], append = TRUE)
  }
  dbCommit(con)

  t_fim_escrita <- Sys.time()
  message("✅ Competência ", competencia, " importada. Tempo de escrita: ",
          round(difftime(t_fim_escrita, t_inicio_escrita, units = "mins"), 2), " minutos")

  if (competencia == splits[[1]]$competencia_arquivo[1]) {
    message("📌 Criando índices...")
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_caged_antigo_municipio_comp ON caged_antigo(municipio, competencia_arquivo)")
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_caged_antigo_cbo ON caged_antigo(cbo_2002_ocupacao)")
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_caged_municipio ON caged_antigo(municipio)")
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_caged_municipio_cbo ON caged_antigo(municipio, cbo_2002_ocupacao)")
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_caged_comp ON caged_antigo(competencia_arquivo)")
  }
}

sanitize_columns <- function(df, competencia) {
  colunas_inteiras <- c(
    "municipio", "ano_declarado", "cbo_2002_ocupacao", "cnae_2_0_classe",
    "faixa_empr_inicio_jan", "grau_instrucao", "qtd_hora_contrat", "ibge_subsetor",
    "idade", "ind_aprendiz", "ind_portador_defic", "raca_cor", "saldo_mov",
    "sexo", "tempo_emprego", "tipo_estab", "tipo_mov_desagregado", "uf",
    "bairros_sp", "bairros_fortaleza", "bairros_rj", "distritos_sp",
    "regioes_adm_df", "mesorregiao", "microrregiao", "regiao_adm_sp",
    "regiao_corede_04", "regiao_gov_sp", "regiao_senac_pr", "regiao_senai_sp",
    "sub_regiao_senai_pr", "ind_trab_parcial", "ind_trab_intermitente"
  )

  df <- df %>%
    mutate(
      across(any_of(colunas_inteiras), ~ suppressWarnings(as.integer(.))),
      salario_mensal = suppressWarnings(as.numeric(gsub(",", ".", salario_mensal))),
      competencia_arquivo = paste0(substr(competencia, 3, 4), substr(competencia, 1, 2))
    )

  return(df)
}

# Executa
arquivos_txt <- list.files(caminho_base, pattern = "\\.txt$", recursive = TRUE, full.names = TRUE)
lapply(arquivos_txt, importar_caged_antigo)

dbDisconnect(con)
cat("🏁 Finalizado: CAGED Antigo importado\n")