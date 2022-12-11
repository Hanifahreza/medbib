from django.apps import AppConfig
import nltk

class MainConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'main'

    def ready(self):
        nltk.download('stopwords')
        nltk.download('punkt')
    
