# airflow
## Start Airflow
We need to execute two steps to build a new container with the Airflow System 

   1 docker compose up airflow-init
   2 docker compose up

Why? I don't know. 

## .ENV

To execute correctly this Airflow in your system, you need to add the .env file using the code:

   $ echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
