import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

# Função para criar a tabela no PostgreSQL
def create_table_from_df(df, schema, table_name, engine):
    dtype_mapping = {
        'object': 'VARCHAR',
        'int64': 'INTEGER',
        'float64': 'NUMERIC',
        'datetime64[ns]': 'TIMESTAMP',
        'bool': 'BOOLEAN',
        'string': 'VARCHAR'
    }
    
    columns_with_types = []
    for column, dtype in df.dtypes.items():
        column_type = dtype_mapping.get(str(dtype), 'VARCHAR')
        if column_type == 'NUMERIC':
            columns_with_types.append(f'"{column}" NUMERIC')
        else:
            columns_with_types.append(f'"{column}" {column_type}')
    
    columns_with_types_str = ", ".join(columns_with_types)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
        id SERIAL PRIMARY KEY,
        {columns_with_types_str}
    );
    """
    
    with engine.connect() as connection:
        try:
            connection.execute(text(create_table_sql))
            print(f"Tabela '{schema}.{table_name}' criada com sucesso.")
        except SQLAlchemyError as e:
            print(f"Erro ao criar a tabela '{schema}.{table_name}': {e}")

# Função para importar dados para a tabela
def import_data_to_table(df, schema, table_name, engine):
    try:
        df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
        print(f"Dados importados para a tabela '{schema}.{table_name}' com sucesso.")
    except Exception as e:
        print(f"Erro ao importar dados para a tabela '{schema}.{table_name}': {e}")

# Configurações de conexão com o banco de dados
db_user = 'usuario'
db_password = 'senha'  # Senha em texto claro
db_host = 'localhost'
db_port = '5432'  # Porta padrão do PostgreSQL
db_name = 'nome_banco'
db_schema = 'schema'

# Codifica a senha para a string de conexão
db_password_encoded = quote_plus(db_password)

# Criação da string de conexão
connection_string = f'postgresql://{db_user}:{db_password_encoded}@{db_host}:{db_port}/{db_name}'
print(f'String de conexão: {connection_string}')

# Criação do engine
engine = create_engine(connection_string)

# Caminho para o arquivo Excel
excel_file_path = 'caminho_para_o_arquivo.xlsx'

# Ler todas as abas em um dicionário de DataFrames
dfs = pd.read_excel(excel_file_path, sheet_name=None)

# Iterar sobre cada aba e criar tabela e importar dados
for sheet_name, df in dfs.items():
    # Verificar e ajustar os dados, se necessário
    df = df.applymap(lambda x: x.encode('utf-8').decode('utf-8') if isinstance(x, str) else x)
    
    # Criar a tabela no banco de dados
    create_table_from_df(df, db_schema, sheet_name, engine)
    
    # Importar os dados para a tabela criada
    import_data_to_table(df, db_schema, sheet_name, engine)
