import pandas as pd 

from flask import Flask, request, jsonify 

from config.DBInterfacePostgres import DBInterface

flask_db_interface = Flask(__name__)
database_object    = DBInterface()
database_object.connection_settings()

@flask_db_interface.route("/upload", methods = ["POST"])
def upload_data():
    table_name = request.form["table_name"]
    dataframe  = pd.read_json(request.files["dataframe"])
    database_object.upload_to_database(table_name, dataframe)
    
    return jsonify({"status" : 'success'})

@flask_db_interface.route("/query", methods = ["GET"])
def query_data():
    table_id         = request.args.get("table_id")
    columns_list     = request.args.get("columns").split(",")
    filter_condition = request.args.get("filter_value")
    result_json_data = pd.DataFrame(database_object.get_from_database(table_id, columns_list, filter_condition))

    return result_json_data.to_json()

@flask_db_interface.route("/delete", methods = ["DELETE"])
def delete_data():
    table_id         = request.args.get("table_id")
    column_name      = request.args.get("column_name")
    filter_value     = request.args.get("filter_value")
    filter_condition = request.args.get("filter_condition")
    database_object.delete_from_database(table_id, column_name, filter_value, filter_condition)

    return jsonify({"status" : "success"})
    
if (__name__ == "__main__"):
    flask_db_interface.run(host = "0.0.0.0", port = 5000, threaded = True)
