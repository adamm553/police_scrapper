import pandas as pd
import requests
import os
from pathlib import Path

class DataScraper:
    def __init__(self):
        self.path = Path(r'C:\Users\a.piatek\Desktop\Bazy\scrapper\webScrapper\pliki\pobrane')
        self.url_dict = {"Ogólnie":'https://statystyka.policja.pl/download/20/232277/przestepstwa-ogolem-do-2021.xlsx',
                    "Bójka_i_pobicie":'https://statystyka.policja.pl/download/20/232198/postepowania-wszczete-przestepstwa-bojka-i-pobicie-do-2021.xlsx',
                    "Uszczerbek_na_zdrowiu":'https://statystyka.policja.pl/download/20/232209/postepowania-wszczete-przestepstwa-uszczerbek-na-zdrowiu-do-2021.xlsx',
                    "Kradzież_cudzej_rzeczy":'https://statystyka.policja.pl/download/20/232212/postepowania-wszczete-przestepstwa-kradziez-cudzej-rzeczy-do-2021.xlsx',
                    "Kradzież_z_włamaniem":'https://statystyka.policja.pl/download/20/232215/postepowania-wszczete-przestepstwa-kradziez-z-wlamaniem-do-2021.xlsx',
                    "Rozbój":'https://statystyka.policja.pl/download/20/232223/postepowania-wszczete-przestepstwa-rozboj-do-2021.xlsx',
                    "Uszkodzenie_rzeczy":'https://statystyka.policja.pl/download/20/232227/postepowania-wszczete-przestepstwa-uszkodzenie-rzeczy-do-2021.xlsx',
                    "Zabójstwo":'https://statystyka.policja.pl/download/20/232256/postepowania-wszczete-przestepstwa-zabojstwo-do-2021.xlsx',
                    "Zgwałcenie":'https://statystyka.policja.pl/download/20/232259/postepowania-wszczete-przestepstwa-zgwalcenie-do-2021.xlsx',
                    "Narkotyki":'https://statystyka.policja.pl/download/20/232264/postepowania-wszczete-przestepstwa-narkotyki-do-2021.xlsx',
                    "Korupcja":'https://statystyka.policja.pl/download/20/232270/postepowania-wszczete-przestepstwa-korupcja-do-2021.xlsx',
                    "Drogowe":'https://statystyka.policja.pl/download/20/232276/postepowania-wszczete-przestepstwa-drogowe-nietrzezwi-do-2021.xlsx'
                    }
        self.path.mkdir(parents=True, exist_ok=True)

    def download_files(self):
            for key, url in self.url_dict.items():
                file_path = self.path / f'przestepstwa_{key}.xlsx'
                if file_path.exists():
                    print(f"{key}: Plik istnieje")
                else:
                    response = requests.get(url)
                    if response.status_code == 200:
                        with open(file_path, 'wb') as f:
                            f.write(response.content)
                        print(f"{key}: Pobrano")
                    else:
                        print(f"{key}: Nie pobrano. Kod błędu {response.status_code}")



