PROJECT='YOUR PROJECT'
BUCKET='YOUR BUCKET'

import json
import gcsfs as gcsfs
import cloudstorage as gcs

class nlp:
    def __init__(self, row):
        self.row = row

    def create_entity_gcp(self):
        from google.oauth2 import service_account
        from google.cloud import language_v1
        
        gcs_file_system = gcsfs.GCSFileSystem(project=PROJECT)
        gcs_json_path = 'gs://{}/key.json'.format(BUCKET)
        with gcs_file_system.open(gcs_json_path) as f:
            json_dict = json.load(f)
    
        credentials = service_account.Credentials.from_service_account_info(json_dict)
    
        try:
            client = language_v1.LanguageServiceClient(credentials=credentials)
            document = language_v1.Document(
                content=self.row,
                type_=language_v1.Document.Type.PLAIN_TEXT)
            
            entities = client.analyze_entities(document=document).entities
            
            for entity in entities:
                Entity_type = language_v1.Entity.Type(entity.type_).name
            return Entity_type
        
        except Exception:
            return 'OTHER'
        
        return Entity_type