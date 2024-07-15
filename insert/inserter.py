import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

class DataLoader:
    def __init__(self, db_url, folder_path):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.folder_path = folder_path
        self.Base = declarative_base()
        self._define_models()
        self.Base.metadata.create_all(self.engine)

    def _define_models(self):
        class Obszar(self.Base):
            __tablename__ = "obszar"
            id_ob = Column(Integer, primary_key=True, autoincrement=True)
            nazwa_ob = Column(String)

        class Przestepstwa(self.Base):
            __tablename__ = "przestepstwa"
            id_przest = Column(Integer, primary_key=True, autoincrement=True)
            id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
            przestepstwo_stw = Column(Integer)
            przestepstwo_wyk = Column(Integer)
            rok = Column(Integer)

        class BojkaPobice(self.Base):
            __tablename__ = "przestepstwa_bojka_i_pobicie"
            id_bojka = Column(Integer, primary_key=True, autoincrement=True)
            id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
            przestepstwa_wyk_bojka = Column(Integer)
            rok_bojka = Column(Integer)

        class Drogowe(self.Base):
            __tablename__ = "przestepstwa_drogowe"
            id_droga = Column(Integer, primary_key=True, autoincrement=True)
            id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
            przestepstwa_wyk_dr = Column(Integer)
            rok_drog = Column(Integer)

        class Korupcyjne(self.Base):
            __tablename__ = "przestepstwa_korupcyjne"
            id_kor = Column(Integer, primary_key=True, autoincrement=True)
            id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
            przestepstwa_wyk_kor = Column(Integer)
            rok_kor = Column(Integer)

        class KradziezCudzej(self.Base):
            __tablename__ = "przestepstwa_kradziez_cudzej"
            id_kr_cudz = Column(Integer, primary_key=True, autoincrement=True)
            id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
            przestepstwa_wyk_kr_cudz = Column(Integer)
            rok_kr_cudz = Column(Integer)

        class KradziezWlamanie(self.Base):
            __tablename__ = "przestepstwa_kradziez_wlamanie"
            id_kr_wlam = Column(Integer, primary_key=True, autoincrement=True)
            id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
            przestepstwa_wyk_kr_wlam = Column(Integer)
            rok_kr_wlam = Column(Integer)

        class Narkotykowe(self.Base):
            __tablename__ = "przestepstwa_narkotykowe"
            id_nark = Column(Integer, primary_key=True, autoincrement=True)
            id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
            rok_nark = Column(Integer)
            przestepstwa_wyk_nark = Column(Integer)

        class Rozboj(self.Base):
            __tablename__ = "przestepstwa_rozboj"
            id_rozboj = Column(Integer, primary_key=True, autoincrement=True)
            id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
            przestepstwa_wyk_rozboj = Column(Integer)
            rok_rozboj = Column(Integer)

        class Uszczerbek(self.Base):
            __tablename__ = "przestepstwa_uszczerbek"
            id_uszczerbek = Column(Integer, primary_key=True, autoincrement=True)
            id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
            przestepstwa_wyk_uszczerbek = Column(Integer)
            rok_uszczerbek = Column(Integer)

        class UszkodzenieMienia(self.Base):
            __tablename__ = "przestepstwa_uszkodzenie_mienia"
            id_uszk = Column(Integer, primary_key=True, autoincrement=True)
            id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
            przestepstwa_wyk_uszk = Column(Integer)
            rok_uszk = Column(Integer)

        class Zabojstwa(self.Base):
            __tablename__ = "przestepstwa_zabojstwa"
            id_zab = Column(Integer, primary_key=True, autoincrement=True)
            id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
            przestepstwa_wyk_zab = Column(Integer)
            rok_zab = Column(Integer)

        class Zgwalcenia(self.Base):
            __tablename__ = "przestepstwa_zgwalcenia"
            id_gwalt = Column(Integer, primary_key=True, autoincrement=True)
            id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
            przestepstwa_wyk_gwalt = Column(Integer)
            rok_gwalt = Column(Integer)

        self.models = {
            'obszar': Obszar,
            'przestepstwa': Przestepstwa,
            'bojka_pobice': BojkaPobice,
            'drogowe': Drogowe,
            'korupcyjne': Korupcyjne,
            'kradziez_cudzej': KradziezCudzej,
            'kradziez_wlamanie': KradziezWlamanie,
            'narkotykowe': Narkotykowe,
            'rozboj': Rozboj,
            'uszczerbek': Uszczerbek,
            'uszkodzenie_mienia': UszkodzenieMienia,
            'zabojstwa': Zabojstwa,
            'zgwalcenia': Zgwalcenia,
        }

    def load_data(self):
        self._load_obszary()
        self._load_data_files()

    def _load_obszary(self):
        obszar_xlsx = os.path.join(self.folder_path, 'a_obszary.xlsx')
        obszar_df = pd.read_excel(obszar_xlsx)

        for index, row in obszar_df.iterrows():
            existing_obszar = self.session.query(self.models['obszar']).filter_by(id_ob=int(row['id_ob'])).first()
            if existing_obszar:
                existing_obszar.nazwa_ob = str(row['nazwa_ob'])
            else:
                obszar = self.models['obszar'](id_ob=int(row['id_ob']), nazwa_ob=str(row['nazwa_ob']))
                self.session.add(obszar)
        self.session.commit()

    def _load_data_files(self):
        files_to_load = [
            ('filtered_przestepstwa_Bójka_i_pobicie.xlsx', 'bojka_pobice', {'id_ob': 'Jednostka organizacyjna Policji', 'rok_bojka': 'Rok'}, {'przestepstwa_wyk_bojka': 'Postępowania wszczęte'}),
            ('filtered_przestepstwa_Drogowe.xlsx', 'drogowe', {'id_ob': 'Jednostka organizacyjna Policji', 'rok_drog': 'Rok'}, {'przestepstwa_wyk_dr': 'Postępowania wszczęte'}),
            ('filtered_przestepstwa_Korupcja.xlsx', 'korupcyjne', {'id_ob': 'Jednostka organizacyjna Policji', 'rok_kor': 'Rok'}, {'przestepstwa_wyk_kor': 'Postępowania wszczęte'}),
            ('filtered_przestepstwa_Kradzież_cudzej_rzeczy.xlsx', 'kradziez_cudzej', {'id_ob': 'Jednostka organizacyjna Policji', 'rok_kr_cudz': 'Rok'}, {'przestepstwa_wyk_kr_cudz': 'Postępowania wszczęte'}),
            ('filtered_przestepstwa_Kradzież_z_włamaniem.xlsx', 'kradziez_wlamanie', {'id_ob': 'Jednostka organizacyjna Policji', 'rok_kr_wlam': 'Rok'}, {'przestepstwa_wyk_kr_wlam': 'Postępowania wszczęte'}),
            ('filtered_przestepstwa_Narkotyki.xlsx', 'narkotykowe', {'id_ob': 'Jednostka organizacyjna Policji', 'rok_nark': 'Rok'}, {'przestepstwa_wyk_nark': 'Postępowania wszczęte'}),
            ('filtered_przestepstwa_Rozbój.xlsx', 'rozboj', {'id_ob': 'Jednostka organizacyjna Policji', 'rok_rozboj': 'Rok'}, {'przestepstwa_wyk_rozboj': 'Postępowania wszczęte'}),
            ('filtered_przestepstwa_Uszczerbek_na_zdrowiu.xlsx', 'uszczerbek', {'id_ob': 'Jednostka organizacyjna Policji', 'rok_uszczerbek': 'Rok'}, {'przestepstwa_wyk_uszczerbek': 'Postępowania wszczęte'}),
            ('filtered_przestepstwa_Uszkodzenie_rzeczy.xlsx', 'uszkodzenie_mienia', {'id_ob': 'Jednostka organizacyjna Policji', 'rok_uszk': 'Rok'}, {'przestepstwa_wyk_uszk': 'Postępowania wszczęte'}),
            ('filtered_przestepstwa_Zabójstwo.xlsx', 'zabojstwa', {'id_ob': 'Jednostka organizacyjna Policji', 'rok_zab': 'Rok'}, {'przestepstwa_wyk_zab': 'Postępowania wszczęte'}),
            ('filtered_przestepstwa_Zgwałcenie.xlsx', 'zgwalcenia', {'id_ob': 'Jednostka organizacyjna Policji', 'rok_gwalt': 'Rok'}, {'przestepstwa_wyk_gwalt': 'Postępowania wszczęte'}),
            ('filtered_przestepstwa_Ogólnie.xlsx', 'przestepstwa', {'id_ob': 'Jednostka podziału administracyjnego', 'rok': 'Rok'}, {'przestepstwo_stw': 'Przestępstwa stwierdzone', 'przestepstwo_wyk': 'Przestępstwa wykryte'})
        ]
        
        for file_name, model_name, filters, update_fields in files_to_load:
            self._upsert_data(file_name, self.models[model_name], filters, update_fields)

    def _upsert_data(self, file_name, model_class, filters, update_fields):
        xlsx_path = os.path.join(self.folder_path, file_name)
        df = pd.read_excel(xlsx_path)
        
        for index, row in df.iterrows():
            filter_conditions = {key: int(row[value]) for key, value in filters.items()}
            existing_record = self.session.query(model_class).filter_by(**filter_conditions).first()
            
            if existing_record:
                for field, value in update_fields.items():
                    setattr(existing_record, field, int(row[value]))
            else:
                new_record = model_class(**{field: int(row[value]) for field, value in {**filters, **update_fields}.items()})
                self.session.add(new_record)
        self.session.commit()

    def close(self):
        self.session.close()
