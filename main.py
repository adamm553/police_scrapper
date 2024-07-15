import os
from dotenv import load_dotenv, dotenv_values
from scraper.scraper import DataScraper
from data_manipulation.id_converter import ExcelProcessor
from data_manipulation.nd_remover import Nd_replace
from insert.inserter import DataLoader

load_dotenv()

db_url = f"postgresql://{os.getenv("USER")}:{os.getenv("PASSWORD")}@{os.getenv("HOST")}:{os.getenv("PORT")}/{os.getenv("DBNAME")}"
folder_path = os.getenv("PATH")



scraper = DataScraper()
scraper.download_files()

id_converter = ExcelProcessor()
id_converter.process_excel_files()

nd = Nd_replace()
nd.load_and_replace_nd()

data_loader = DataLoader(db_url, folder_path)
data_loader.load_data()
data_loader.close()

