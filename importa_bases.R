# Carregar pacotes necessários
if (!require("RCurl")) install.packages("RCurl", dependencies = TRUE)
if (!require("stringr")) install.packages("stringr", dependencies = TRUE)
if (!require("archive")) install.packages("archive", dependencies = TRUE)
if (!require("progress")) install.packages("progress", dependencies = TRUE)

library(RCurl)
library(stringr)
library(archive)
library(progress)

options(timeout = 300)

listar_ftp <- function(url) {
  tryCatch(
    {
      linhas <- strsplit(getURL(url, dirlistonly = TRUE), "\r*\n")[[1]]
      linhas <- trimws(linhas)
      linhas <- linhas[linhas != "" & !is.na(linhas)]
      return(linhas)
    },
    error = function(e) {
      cat("Erro ao acessar:", url, "\n")
      return(character(0))
    }
  )
}

importa_bases <- function(tipo = c("NOVO_CAGED", "CAGED", "AMBOS")) {
  tipo <- match.arg(tipo)

  if (tipo == "AMBOS") {
    importa_bases("NOVO_CAGED")
    importa_bases("CAGED")
    return(invisible(NULL))
  }

  ftp_base <- if (tipo == "NOVO_CAGED") {
    "ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/"
  } else {
    "ftp://ftp.mtps.gov.br/pdet/microdados/CAGED/"
  }

  dir_local <- if (tipo == "NOVO_CAGED") {
    "dados_novo_caged"
  } else {
    "dados_caged_antigo"
  }

  if (!dir.exists(dir_local)) dir.create(dir_local)

  anos <- listar_ftp(ftp_base)
  anos <- if (tipo == "NOVO_CAGED") {
    anos[str_detect(anos, "^20[2-9][0-9]$")]
  } else {
    anos[str_detect(anos, "^20(0[7-9]|1[0-9])$")]
  }

  total_arquivos <- 0
  for (ano in anos) {
    ano_url <- paste0(ftp_base, ano, "/")
    arquivos <- listar_ftp(ano_url)
    if (tipo == "NOVO_CAGED") {
      meses <- arquivos[str_detect(arquivos, "^[0-9]{6}$")]
      for (mes in meses) {
        mes_url <- paste0(ano_url, mes, "/")
        arquivos_mes <- listar_ftp(mes_url)
        arquivos_7z <- arquivos_mes[str_detect(arquivos_mes, "\\.7z$")]
        total_arquivos <- total_arquivos + length(arquivos_7z)
      }
    } else {
      arquivos_7z <- arquivos[str_detect(arquivos, "\\.7z$")]
      total_arquivos <- total_arquivos + length(arquivos_7z)
    }
  }

  pb <- progress_bar$new(
    format = "[:bar] :percent :message",
    total = total_arquivos,
    clear = FALSE,
    width = 80
  )

  for (ano in anos) {
    ano_url <- paste0(ftp_base, ano, "/")
    ano_local <- file.path(dir_local, ano)
    if (!dir.exists(ano_local)) dir.create(ano_local)

    if (tipo == "NOVO_CAGED") {
      meses <- listar_ftp(ano_url)
      meses <- meses[str_detect(meses, "^[0-9]{6}$")]

      for (mes in meses) {
        mes_url <- paste0(ano_url, mes, "/")
        mes_local <- file.path(ano_local, mes)
        if (!dir.exists(mes_local)) dir.create(mes_local)

        arquivos <- listar_ftp(mes_url)
        arquivos_7z <- arquivos[str_detect(arquivos, "\\.7z$")]

        for (arquivo in arquivos_7z) {
          arquivo_url <- paste0(mes_url, arquivo)
          arquivo_7z <- file.path(mes_local, arquivo)
          arquivo_txt <- file.path(mes_local, paste0(tools::file_path_sans_ext(arquivo), ".txt"))
          arquivo_local <- arquivo_7z

          if (file.exists(arquivo_txt)) {
            if (file.exists(arquivo_7z)) {
              tryCatch(file.remove(arquivo_7z), error = function(e) FALSE)
            }
            pb$tick(tokens = list(message = paste("Pulando", arquivo)))
            next
          }

          tryCatch(
            {
              download.file(arquivo_url, arquivo_local, mode = "wb")
              pb$tick(tokens = list(message = paste("Baixado", arquivo)))
            },
            error = function(e) {
              pb$tick(tokens = list(message = paste("Erro", arquivo)))
              next
            }
          )
          if (file.exists(arquivo_local)) {
            tryCatch(
              {
                archive_extract(arquivo_local, dir = mes_local)
                file.remove(arquivo_local)
              },
              error = function(e) {
                cat("Erro ao descompactar:", arquivo_local, "\n")
              }
            )
          }
        }
      }
    } else {
      arquivos <- listar_ftp(ano_url)
      arquivos_7z <- arquivos[str_detect(arquivos, "\\.7z$")]
      for (arquivo in arquivos_7z) {
        arquivo_url <- paste0(ano_url, arquivo)
        arquivo_local <- file.path(ano_local, arquivo)

        arquivo_7z <- file.path(ano_local, arquivo)
        arquivo_txt <- file.path(ano_local, paste0(tools::file_path_sans_ext(arquivo), ".txt"))

        if (file.exists(arquivo_txt)) {
          if (file.exists(arquivo_7z)) {
            tryCatch(file.remove(arquivo_7z), error = function(e) FALSE)
          }
          pb$tick(tokens = list(message = paste("Pulando", arquivo)))
          next
        }

        tryCatch(
          {
            download.file(arquivo_url, arquivo_local, mode = "wb")
            pb$tick(tokens = list(message = paste("Baixado", arquivo)))
          },
          error = function(e) {
            pb$tick(tokens = list(message = paste("Erro", arquivo)))
            next
          }
        )

        if (file.exists(arquivo_local)) {
          tryCatch(
            {
              archive_extract(arquivo_local, dir = ano_local)
              file.remove(arquivo_local)
            },
            error = function(e) {
              cat("Erro ao descompactar:", arquivo_local, "\n")
            }
          )
        }
      }
    }
  }

  cat("\n✅ Processo concluído com sucesso para", tipo, "!\n")
}