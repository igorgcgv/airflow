# Projeto ETL com Airflow
Este é um projeto que demonstra como criar pipelines de ETL (Extract, Transform, Load) usando o Apache Airflow. O Airflow é uma plataforma de orquestração de fluxo de trabalho de código aberto que permite agendar, monitorar e gerenciar pipelines de dados de maneira eficiente.

# Visão Geral
Neste projeto, desenvolvemos pipelines de ETL para extrair dados de uma fonte de dados, transformá-los de acordo com nossos requisitos e carregá-los em um destino. Os pipelines são definidos como DAGs (Directed Acyclic Graphs) no Airflow, onde cada tarefa representa uma etapa do processo de ETL.

# Estrutura do Projeto
O projeto está estruturado da seguinte forma:

* dags/: Contém os arquivos Python que definem os DAGs do Airflow;
* data/: Diretório para armazenar dados de entrada e saída;
* scripts/: Scripts auxiliares para execução e manipulação de dados;

# Requisitos
* Python 3.x
* Apache Airflow
