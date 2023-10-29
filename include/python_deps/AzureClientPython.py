from azure.data.tables import TableServiceClient


class PythonAzureClient:
    def __init__(self):
        self.table_service = None
        self.table_clients = {}

    def create_conn(self, conn_str):
        self.table_service = TableServiceClient.from_connection_string(conn_str)

    def range_search(self, tbl_name, part, low_key, high_key):
        table_client = self.table_clients.get(tbl_name, None)
        if table_client is None:
            table_client = self.table_service.get_table_client(tbl_name)
            self.table_clients[tbl_name] = table_client
        filters = [
            "PartitionKey eq '{}'".format(part),
            "RowKey ge '{}'".format(low_key),
            "RowKey le '{}'".format(high_key),
        ]
        result = list(table_client.query_entities(" and ".join(filters)))
        return result
