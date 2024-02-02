# import libraries
import pathlib
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata


# connections & variables
POSTGRES_CONN_ID = "DWDB"


# default args and init dag
CWD = pathlib.Path(__file__).parent
default_args = {
    "owner": "Gustavo Souza",
    "retries": 1,
    "retry_delay": 0,
}

# declare dag
@dag(
    dag_id="data_load_warehouse",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['postgres', 'csv', 'dw', 'astrosdk'],
    default_args=default_args    
)

# declare main function
def load_files_warehouse():

    # init & finish
    init = EmptyOperator(task_id="init")
    finish = EmptyOperator(task_id="finish")

    # TODO load dimensions csv file from data lake to warehouse systems

    load_dim_cliente = aql.load_file(
        task_id="load_dim_cliente",
        input_file=File(path=str(CWD.parent) + "/include/dados/DIM_CLIENTE*", filetype=FileType.CSV, conn_id=POSTGRES_CONN_ID),
        output_table=Table(name="dim_cliente", conn_id=POSTGRES_CONN_ID, metadata=Metadata(schema="dw")),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    load_dim_deposito = aql.load_file(
        task_id="load_dim_deposito",
        input_file=File(path=str(CWD.parent) + "/include/dados/DIM_DEPOSITO*", filetype=FileType.CSV, conn_id=POSTGRES_CONN_ID),
        output_table=Table(name="dim_deposito", conn_id=POSTGRES_CONN_ID, metadata=Metadata(schema="dw")),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    load_dim_entrega = aql.load_file(
        task_id="load_dim_entrega",
        input_file=File(path=str(CWD.parent) + "/include/dados/DIM_ENTREGA*", filetype=FileType.CSV, conn_id=POSTGRES_CONN_ID),
        output_table=Table(name="dim_entrega", conn_id=POSTGRES_CONN_ID, metadata=Metadata(schema="dw")),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    load_dim_frete = aql.load_file(
        task_id="load_dim_frete",
        input_file=File(path=str(CWD.parent) + "/include/dados/DIM_FRETE*", filetype=FileType.CSV, conn_id=POSTGRES_CONN_ID),
        output_table=Table(name="dim_frete", conn_id=POSTGRES_CONN_ID, metadata=Metadata(schema="dw")),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    load_dim_pagamento = aql.load_file(
        task_id="load_dim_pagamento",
        input_file=File(path=str(CWD.parent) + "/include/dados/DIM_PAGAMENTO*", filetype=FileType.CSV, conn_id=POSTGRES_CONN_ID),
        output_table=Table(name="dim_pagamento", conn_id=POSTGRES_CONN_ID, metadata=Metadata(schema="dw")),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    load_dim_transportadora = aql.load_file(
        task_id="load_dim_transportadora",
        input_file=File(path=str(CWD.parent) + "/include/dados/DIM_TRANSPORTADORA*", filetype=FileType.CSV, conn_id=POSTGRES_CONN_ID),
        output_table=Table(name="dim_transportadora", conn_id=POSTGRES_CONN_ID, metadata=Metadata(schema="dw")),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    load_tb_fato = aql.load_file(
        task_id="load_tb_fato",
        input_file=File(path=str(CWD.parent) + "/include/dados/TB_FATO*", filetype=FileType.CSV, conn_id=POSTGRES_CONN_ID),
        output_table=Table(name="tb_fato", conn_id=POSTGRES_CONN_ID, metadata=Metadata(schema="dw")),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # define sequence
    init >> [load_dim_cliente, load_dim_deposito, load_dim_entrega, load_dim_frete, load_dim_pagamento, load_dim_transportadora ] >> load_tb_fato >> finish

# init
dag = load_files_warehouse()
