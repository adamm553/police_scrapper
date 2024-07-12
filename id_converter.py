import pandas as pd
import os

class ExcelProcessor:
    download_path = r'C:\Users\a.piatek\Desktop\Bazy\scrapper\webScrapper\pliki\pobrane'
    new_path = r'C:\Users\a.piatek\Desktop\Bazy\scrapper\webScrapper\pliki\przerobione'

    def process_excel_files(self):
        if not os.path.exists(ExcelProcessor.download_path):
            os.makedirs(ExcelProcessor.download_path)

        files = os.listdir(ExcelProcessor.download_path)
        excel_files = [file for file in files if file.endswith('.xlsx')]

        for file in excel_files:
            file_path = os.path.join(ExcelProcessor.download_path, file)

            df = pd.read_excel(file_path, header=2)

            possible_column_names = ['Jednostka organizacyjna Policji', 'Jednostka podziału administracyjnego']
            org_unit_column = next((col for col in possible_column_names if col in df.columns), None)

            if org_unit_column is None:
                continue

            unique_names = df[org_unit_column].unique()
            name_to_id = {name: idx + 1 for idx, name in enumerate(unique_names)}

            df[org_unit_column] = df[org_unit_column].map(name_to_id)

            df_filtered = df[df[org_unit_column] <= 17]

            output_file_path = os.path.join(ExcelProcessor.new_path, f"filtered_{file}")

            df_filtered.to_excel(output_file_path, index=False)

# Example usage:
if __name__ == "__main__":
    excel_processor = ExcelProcessor()
    excel_processor.process_excel_files()
