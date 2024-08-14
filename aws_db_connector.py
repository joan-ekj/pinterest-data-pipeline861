
import sqlalchemy
import yaml


class AWSDBConnector:

    def __init__(self, creds):
        """
        Initialises the AWSDBConnector with credentials from a YAML file.

        Args:
            creds (str): Path to a YAML file containing database credentials.
        """

        self.creds = self.read_db_creds(creds)

    def read_db_creds(self, creds):
      '''
      This method reads the database credentials from a YAML file.
      
      Args:
        creds(str): Path to the YAML file containing the database credentials.

      Returns: 
        dict: Database credentials.

      '''
      with open(creds, 'r') as db_cred:
        credentials = yaml.safe_load(db_cred)
      return credentials
        
    def create_db_connector(self):
        '''
       This method initialises the SQLAlchemy database engine using the credentials.

       Args:
        None

       Returns:
        engine: SQLAlchemy engine.

       '''
        creds = self.creds
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{creds.USER}:{creds.PASSWORD}@{creds.HOST}:{creds.PORT}/{creds.DATABASE}?charset=utf8mb4")
        return engine
    