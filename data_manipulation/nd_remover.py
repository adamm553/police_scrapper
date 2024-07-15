import pandas as pd 


class Nd_replace():
    def load_and_replace_nd(self):
            file_path = r"C:\Users\a.piatek\Desktop\Bazy\scrapper\webScrapper\pliki\przerobione\filtered_przestepstwa_Drogowe.xlsx"
            df = pd.read_excel(file_path)
            
            df.iloc[:, 2] = df.iloc[:, 2].replace('n.d.', 0)
            df.to_excel(file_path, index=False)

