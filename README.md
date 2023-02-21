# TechIndicium
<h2>  Desafio Engenharia de Dados </h2> 

O data pipeline ficou dividido em 4 tarefas sequenciais: 
  - Get_name_all_tables: pega os nomes de todas as tabelas do banco de dados.
  - Get_all_tables_data: a qual pega todos os dados de todas as tabelas com as suas colunas cria um pandas dataframe e transforma ele em csv.
  - create_output_tables: cria o banco de respostas apenas. Com o código do banco localizado na pasta: 
  
    desafio/dags/airflow_results_db.sql
  - insert_in_output_tables: a função que insere os dados vindos do csv para o banco de respostas. Esse possui uma conexão com o banco de inputs para pegar o nome de cada coluna das tabelas para utilizar no copy_from(), para fazer uma conexão por postgresHook com o banco de respostas e inseri-las com o copy_from().

<img src = https://user-images.githubusercontent.com/46203330/220226885-a088a002-ca73-43e4-9f2a-d35d8c568412.jpg />

<h3> Bancos de Dados </h3>

Foram criadas 3 bancos de dados: Um banco para o airflow, um banco para as entradas , Northwind, e outro banco de dados para a saída dos dados: db_results.

<img src = https://user-images.githubusercontent.com/46203330/220227658-d25695a2-daac-40d7-a7d2-02a306a2edde.png />

<h3> Data </h3>

Todos os arquivos CSVs foram armazenados na pasta de dados, os arquivos CSVs que foram gerados para serem inseridos no banco estão dentro da pasta
    desafio/data/csv/

<img src = https://user-images.githubusercontent.com/46203330/220230589-ce76c639-01aa-49a1-818a-8c2d42b75e2d.png />

<h3> Docker </h3>
Para inicializar o projeto, é necessário dar docker-compose up dentro da pasta Desafio.
