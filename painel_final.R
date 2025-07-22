if (!require("shiny")) install.packages("shiny")
if (!require("DT")) install.packages("DT")

library(shiny)
library(DT)
library(readr)

dados <- read_csv("resultado_desastres_hidrologicos_vs_admissoes.csv")

ui <- fluidPage(
  titlePanel("Exploração dos Desastres por Município"),
  DTOutput("tabela")
)

server <- function(input, output, session) {
  output$tabela <- renderDT({
    datatable(dados, options = list(pageLength = 50, scrollX = TRUE), filter = "top")
  })
}

runApp(
  list(
    ui = ui,
    server = server
  ),
  launch.browser = TRUE
)