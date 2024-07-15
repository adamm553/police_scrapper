from scraper.scraper import DataScraper
from data_manipulation.id_converter import ExcelProcessor
from data_manipulation.nd_remover import Nd_replace
from insert.inserter import DataLoader


host = 'staz-wasko.postgres.database.azure.com'
dbname = 'przestepstwa'
user = "staz"
password = "WaskoCoigGliwiceKatowice1"
port = 5432

db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
folder_path = r"C:\Users\a.piatek\Desktop\Bazy\scrapper\webScrapper\pliki\przerobione"



scraper = DataScraper()
scraper.download_files()

id_converter = ExcelProcessor()
id_converter.process_excel_files()

nd = Nd_replace()
nd.load_and_replace_nd()

data_loader = DataLoader(db_url, folder_path)
data_loader.load_data()
data_loader.close()

