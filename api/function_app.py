import azure.functions as func
import logging
import pymssql
import json

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración de la base de datos de Azure SQL
server = 'globant-bd-poc.database.windows.net'  # Tu servidor Azure SQL
database = 'globant-bd-poc'  # Nombre de tu base de datos
username = 'globant-bd-poc'  # Tu nombre de usuario
password = 'Peru012025'  # Tu contraseña

# Diccionario de reglas de datos para cada tabla
datatype_rules = {
    'jobs': ['int', 'string'],  # [id (int), job (string)]
    'departments': ['int', 'string'],  # [id (int), department (string)]
    'hired_employees': ['int', 'string', 'datetime', 'int', 'int']  # [id (int), name (string), datetime (datetime), department_id (int), job_id (int)]
}

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="api_data/insert_data", methods=["POST"])
def insert_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Obtener los datos del cuerpo de la solicitud (suponiendo que los datos sean JSON)
        req_body = req.get_json()
        table_name = req_body.get('table')
        data = req_body.get('data')

        # Validar que se ha proporcionado la tabla y los datos
        if not table_name or not data:
            return func.HttpResponse("Missing table name or data", status_code=400)

        # Validar que los datos estén dentro del rango permitido (hasta 1000 filas)
        if len(data) > 1000:
            return func.HttpResponse("Cannot insert more than 1000 rows at a time", status_code=400)

        # Validar que los datos cumplan con las reglas del diccionario de datos
        if table_name not in datatype_rules:
            return func.HttpResponse(f"Unknown table {table_name}", status_code=400)

        invalid_records = []  # Lista para almacenar los registros inválidos
        valid_records = []  # Lista para almacenar los registros válidos

        for i, record in enumerate(data):
            if len(record) != len(datatype_rules[table_name]):
                invalid_records.append({
                    "record": record,
                    "error_message": f"Invalid data for record, incorrect number of fields"
                })
                continue

            error_messages = []  # Lista para acumular mensajes de error para un solo registro

            # Validar tipo de dato de cada campo
            for j, field in enumerate(record):
                expected_type = datatype_rules[table_name][j]
                if expected_type == 'int' and not isinstance(field, int):
                    error_messages.append(f"Field {j+1} should be of type int")
                if expected_type == 'string' and not isinstance(field, str):
                    error_messages.append(f"Field {j+1} should be of type string")
                if expected_type == 'datetime' and not isinstance(field, str):  # Assuming ISO format datetime string
                    error_messages.append(f"Field {j+1} should be of type datetime")

            # Si hay mensajes de error, concatenarlos y agregar al registro inválido
            if error_messages:
                invalid_records.append({
                    "record": record,
                    "error_message": " | ".join(error_messages)  # Concatenar todos los errores
                })
            else:
                # Si el registro es válido, agregarlo a la lista de registros válidos
                valid_records.append(record)

        # Si hay registros inválidos, insertarlos en la tabla de errores
        if invalid_records:
            conn = pymssql.connect(server=server, user=username, password=password, database=database)
            cursor = conn.cursor()

            for invalid_record in invalid_records:
                cursor.execute("INSERT INTO dbo.error_logs (table_name, invalid_data, error_message) VALUES (%s, %s, %s)",
                               (table_name, json.dumps(invalid_record["record"]), invalid_record["error_message"]))

            conn.commit()
            conn.close()

            # Responder con un mensaje que los registros inválidos fueron almacenados
            # return func.HttpResponse(f"Invalid data stored in error_logs. Total invalid records: {len(invalid_records)}", status_code=400)

        # Si hay registros válidos, proceder con la inserción
        if valid_records:
            conn = pymssql.connect(server=server, user=username, password=password, database=database)
            cursor = conn.cursor()

            # Inserción de registros válidos
            if table_name == 'jobs':
                for record in valid_records:
                    cursor.execute("INSERT INTO dbo.jobs (id, job) VALUES (%d, %s)", (record[0], record[1]))
            elif table_name == 'departments':
                for record in valid_records:
                    cursor.execute("INSERT INTO dbo.departments (id, department) VALUES (%d, %s)", (record[0], record[1]))
            elif table_name == 'hired_employees':
                for record in valid_records:
                    cursor.execute("INSERT INTO dbo.hired_employees (id, name, datetime, department_id, job_id) VALUES (%d, %s, %s, %d, %d)",
                                   (record[0], record[1], record[2], record[3], record[4]))

            conn.commit()
            conn.close()

            # Responder con el mensaje de éxito
    
        else:
            return func.HttpResponse(f"No valid data to insert | Total invalid records: {len(invalid_records)} ", status_code=400)

        
        return func.HttpResponse(f"Data inserted successfully into {table_name} | Total valid records: {len(invalid_records)} | Total invalid records: {len(invalid_records)}", status_code=200)

    except Exception as e:
        logger.error(f"Error while inserting data: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@app.route(route="api_data2/get_data_base1", methods=["GET"])
def get_data_base1(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Establecer conexión con la base de datos SQL utilizando pymssql
        conn = pymssql.connect(server=server, user=username, password=password, database=database)
        cursor = conn.cursor()

        query = """
                SELECT department, job, q1, q2, q3, q4
                FROM
                (SELECT 
                    d.department,
                    j.job,
                    SUM(CASE WHEN MONTH(he.datetime) BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Q1,
                    SUM(CASE WHEN MONTH(he.datetime) BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS Q2,
                    SUM(CASE WHEN MONTH(he.datetime) BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS Q3,
                    SUM(CASE WHEN MONTH(he.datetime) BETWEEN 10 AND 12 THEN 1 ELSE 0 END) AS Q4,
                    SUM(CASE WHEN MONTH(he.datetime) BETWEEN 1 AND 12 THEN 1 ELSE 0 END) AS TOTAL
                FROM hired_employees he
                JOIN departments d ON he.department_id = d.id
                JOIN jobs j ON he.job_id = j.id
                WHERE YEAR(he.datetime) = 2021
                GROUP BY d.department, j.job) a
                ORDER BY TOTAL DESC;
                """

        cursor.execute(query)
        rows = cursor.fetchall()

        # Si hay resultados en la consulta
        if rows:
            # Convertir los resultados a un formato JSON
            result = json.dumps([dict(zip([column[0] for column in cursor.description], row)) for row in rows])
        else:
            result = "No data found."

        conn.close()

        # Devolver los resultados en formato JSON
        return func.HttpResponse(result, mimetype="application/json", status_code=200)

    except Exception as e:
        logger.error(f"Error while retrieving data: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

@app.route(route="api_data2/get_data_base2", methods=["GET"])
def get_data_base2(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Establecer conexión con la base de datos SQL utilizando pymssql
        conn = pymssql.connect(server=server, user=username, password=password, database=database)
        cursor = conn.cursor()

        query = """
                SELECT dh.department_id, dh.department, dh.hired, pe.prom_emp
                FROM (
                    SELECT 
                        d.id AS department_id, 
                        d.department, 
                        COUNT(*) AS hired
                    FROM hired_employees he
                    JOIN departments d ON he.department_id = d.id
                    WHERE YEAR(he.datetime) = 2021
                    GROUP BY d.id, d.department
                ) dh,
                (
                    SELECT AVG(p_count) AS prom_emp
                    FROM (
                        SELECT COUNT(1) AS p_count 
                        FROM hired_employees he
                        JOIN departments d ON he.department_id = d.id
                        WHERE YEAR(he.datetime) = 2021
                        GROUP BY d.id
                    ) hd
                ) pe
                WHERE dh.hired > pe.prom_emp;
                """
        
        cursor.execute(query)
        rows = cursor.fetchall()

        # Si hay resultados en la consulta
        if rows:
            # Convertir los resultados a un formato JSON
            result = json.dumps([dict(zip([column[0] for column in cursor.description], row)) for row in rows])
        else:
            result = "No data found."

        conn.close()

        # Devolver los resultados en formato JSON
        return func.HttpResponse(result, mimetype="application/json", status_code=200)

    except Exception as e:
        logger.error(f"Error while retrieving data: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)