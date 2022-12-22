
# Create a BigQuery client class using service account credentials and returns dataframe based on SQL query.

from google.cloud import bigquery

class SQLFileNotFound(Exception):
    """Raised when SQL file is not found."""
    pass

class BigQueryClient:
    def __init__(self, credentials_file, project_id):
        """Initialize BigQuery client using service account credentials and project ID."""
        self.credentials_file = credentials_file
        self.project_id = project_id
        self.default_days = 30

    def get_client(self):
        """Returns BigQuery client."""
        return bigquery.Client.from_service_account_json(self.credentials_file)

    def get_dataframe(self, query):
        """Returns dataframe based on SQL query."""
        return self.get_client().query(query).to_dataframe()

    def get_table(self, table_id):
        """Returns table based on table ID."""
        return self.get_client().get_table(table_id)
    
    def get_dataset(self, dataset_id):
        """Returns dataset based on dataset ID."""
        return self.get_client().get_dataset(dataset_id)

    
    def run_query_with_params(self, query_name, params):
        """loads SQL query from file in sql folder and returns dataframe."""
        try:
            with open('lib/sql/{}.sql'.format(query_name), 'r') as f:
                query = f.read()
            return self.get_dataframe(query.format(**params))
        except SQLFileNotFound:
            print("SQL file not found.")
        
        return None

    def run_query(self, query_name, dataset_id, num_days = None):
        """Passes valued to run_query_with_params and returns dataframe."""
        if num_days is None:
            num_days = self.default_days
        params = {
            'dataset_id': dataset_id,
            'project_id': self.project_id,
            'num_days': num_days
        }
        return self.run_query_with_params(query_name, params)

    


