import psycopg2
from sqlalchemy import create_engine, Column, Integer, String, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import ForeignKey
import pandas as pd
import os

#Daj to do plików konfiguracyjnych
host = 'staz-wasko.postgres.database.azure.com'
dbname = 'postgres'
user = "staz"
password = "WaskoCoigGliwiceKatowice1"
port = 5432


db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(db_url)

Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

class Obszar(Base):
    __tablename__ = "obszar"
    id_ob = Column(Integer, primary_key = True, autoincrement=True)
    nazwa_ob = Column(String)

class Przestepstwa(Base):
    __tablename__ = "przestepstwa"
    id_przest = Column(Integer, primary_key = True, autoincrement=True)
    id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
    przestepstwo_stw = Column(Integer)
    przestepstwo_wyk = Column(Integer)
    rok = Column(Integer)

class BojkaPobice(Base):
    __tablename__ = "przestepstwa_bojka_i_pobicie"
    id_bojka = Column(Integer, primary_key = True, autoincrement=True)
    id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
    przestepstwa_wyk_bojka = Column(Integer)
    rok_bojka = Column(Integer)

class Drogowe(Base):
    __tablename__ = "przestepstwa_drogowe"
    id_droga = Column(Integer, primary_key = True, autoincrement=True)
    id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
    przestepstwa_wyk_dr = Column(Integer)
    rok_drog = Column(Integer)

class Korupcyjne(Base):
    __tablename__ = "przestepstwa_korupcyjne"
    id_kor = Column(Integer, primary_key=True, autoincrement=True)
    id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
    przestepstwa_wyk_kor = Column(Integer)
    rok_kor = Column(Integer)

class KradziezCudzej(Base):
    __tablename__ = "przestepstwa_kradziez_cudzej" 
    id_kr_cudz = Column(Integer, primary_key=True, autoincrement=True)
    id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
    przestepstwa_wyk_kr_cudz = Column(Integer)
    rok_kr_cudz = Column(Integer)

class KradziezWlamanie(Base):
    __tablename__ = "przestepstwa_kradziez_wlamanie"
    id_kr_wlam = Column(Integer, primary_key=True, autoincrement=True)
    id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
    przestepstwa_wyk_kr_wlam = Column(Integer)
    rok_kr_wlam = Column(Integer)

class Narkotykowe(Base):
    __tablename__ = "przestepstwa_narkotykowe"
    id_nark = Column(Integer, primary_key=True, autoincrement=True)
    id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
    rok_nark = Column(Integer)
    przestepstwa_wyk_nark = Column(Integer) 

class Rozboj(Base):
    __tablename__ = "przestepstwa_rozboj"
    id_rozboj = Column(Integer, primary_key=True, autoincrement=True)
    id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
    przestepstwa_wyk_rozboj = Column(Integer)
    rok_rozboj = Column(Integer)

class Uszczerbek(Base):
    __tablename__ = "przestepstwa_uszczerbek"
    id_uszczerbek = Column(Integer, primary_key=True, autoincrement=True)
    id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
    przestepstwa_wyk_uszczerbek = Column(Integer)
    rok_uszczerbek = Column(Integer)

class UszkodzenieMienia(Base):
    __tablename__ = "przestepstwa_uszkodzenie_mienia"
    id_uszk = Column(Integer, primary_key=True, autoincrement=True)
    id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
    przestepstwa_wyk_uszk = Column(Integer)
    rok_uszk = Column(Integer)

class Zabojstwa(Base):
    __tablename__ = "przestepstwa_zabojstwa"
    id_zab = Column(Integer, primary_key=True, autoincrement=True)
    id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
    przestepstwa_wyk_zab = Column(Integer)
    rok_zab = Column(Integer)

class Zgwalcenia(Base):
    __tablename__ = "przestepstwa_zgwalcenia"
    id_gwalt = Column(Integer, primary_key=True, autoincrement=True)
    id_ob = Column(Integer, ForeignKey("obszar.id_ob"))
    przestepstwa_wyk_gwalt = Column(Integer)
    rok_gwalt = Column(Integer) 

Base.metadata.create_all(engine)



folder_path = r"C:\Users\a.piatek\Desktop\Bazy\scrapper\webScrapper\pliki\przerobione"


def load_data():
    obszar_xlsx = os.path.join(folder_path, 'a_obszary.xlsx')
    obszar_df = pd.read_excel(obszar_xlsx)

    for index, row in obszar_df.iterrows():
        obszar = Obszar(id_ob=int(row['id_ob']),nazwa_ob=str(row['nazwa_ob']))
        session.add(obszar)
    
    session.commit()
    
    bojka_xlsx = os.path.join(folder_path, 'filtered_przestepstwa_Bójka_i_pobicie.xlsx')
    bojka_df = pd.read_excel(bojka_xlsx)

    for index, row in bojka_df.iterrows():
        bojka = BojkaPobice(id_ob=int(row['Jednostka organizacyjna Policji']), przestepstwa_wyk_bojka=int(row['Postępowania wszczęte']), rok_bojka=int(row['Rok']))
        session.add(bojka)

    session.commit()

    drogowe_xlsx = os.path.join(folder_path, 'filtered_przestepstwa_Drogowe.xlsx')
    drogowe_df = pd.read_excel(drogowe_xlsx)

    for index, row in drogowe_df.iterrows():
        drogowe = Drogowe(id_ob=int(row['Jednostka organizacyjna Policji']), przestepstwa_wyk_dr=int(row['Postępowania wszczęte']), rok_drog=int(row['Rok']))
        session.add(drogowe)

    session.commit()

    korupcja_xlsx = os.path.join(folder_path, 'filtered_przestepstwa_Korupcja.xlsx')
    korupcja_df = pd.read_excel(korupcja_xlsx)

    for index, row in korupcja_df.iterrows():
        korupcja = Korupcyjne(id_ob=int(row['Jednostka organizacyjna Policji']), przestepstwa_wyk_kor=int(row['Postępowania wszczęte']), rok_kor=int(row['Rok']))
        session.add(korupcja)

    session.commit()

    kradziez_cudzej_xlsx = os.path.join(folder_path, 'filtered_przestepstwa_Kradzież_cudzej_rzeczy.xlsx')
    kradziez_cudzej_df = pd.read_excel(kradziez_cudzej_xlsx)

    for index, row in kradziez_cudzej_df.iterrows():
        cudz = KradziezCudzej(id_ob=int(row['Jednostka organizacyjna Policji']), przestepstwa_wyk_kr_cudz=int(row['Postępowania wszczęte']), rok_kr_cudz=int(row['Rok']))
        session.add(cudz)

    session.commit()

    kradziez_wlam_xlsx = os.path.join(folder_path, 'filtered_przestepstwa_Kradzież_z_włamaniem.xlsx')
    kradziez_wlam_df = pd.read_excel(kradziez_wlam_xlsx)

    for index, row in kradziez_wlam_df.iterrows():
        wlam = KradziezWlamanie(id_ob=int(row['Jednostka organizacyjna Policji']), przestepstwa_wyk_kr_wlam=int(row['Postępowania wszczęte']), rok_kr_wlam=int(row['Rok']))
        session.add(wlam)

    session.commit()

    narkotyki_xlsx = os.path.join(folder_path, 'filtered_przestepstwa_Narkotyki.xlsx')
    narkotyki_df = pd.read_excel(narkotyki_xlsx)

    for index, row in narkotyki_df.iterrows():
        nark = Narkotykowe(id_ob=int(row['Jednostka organizacyjna Policji']), przestepstwa_wyk_nark=int(row['Postępowania wszczęte']), rok_nark=int(row['Rok']))
        session.add(nark)

    session.commit()

    rozboj_xlsx = os.path.join(folder_path, 'filtered_przestepstwa_Rozbój.xlsx')
    rozboj_df = pd.read_excel(rozboj_xlsx)

    for index, row in rozboj_df.iterrows():
        rozb = Rozboj(id_ob=int(row['Jednostka organizacyjna Policji']), przestepstwa_wyk_rozboj=int(row['Postępowania wszczęte']), rok_rozboj=int(row['Rok']))
        session.add(rozb)

    session.commit()

    uszczerbek_xlsx = os.path.join(folder_path, 'filtered_przestepstwa_Uszczerbek_na_zdrowiu.xlsx')
    uszczerbek_df = pd.read_excel(uszczerbek_xlsx)

    for index, row in uszczerbek_df.iterrows():
        uszcz = Uszczerbek(id_ob=int(row['Jednostka organizacyjna Policji']), przestepstwa_wyk_uszczerbek=int(row['Postępowania wszczęte']), rok_uszczerbek=int(row['Rok']))
        session.add(uszcz)

    session.commit()

    uszkodzenie_xlsx = os.path.join(folder_path, 'filtered_przestepstwa_Uszkodzenie_rzeczy.xlsx')
    uszkodzenie_df = pd.read_excel(uszkodzenie_xlsx)

    for index, row in uszkodzenie_df.iterrows():
        uszk = UszkodzenieMienia(id_ob=int(row['Jednostka organizacyjna Policji']), przestepstwa_wyk_uszk=int(row['Postępowania wszczęte']), rok_uszk=int(row['Rok']))
        session.add(uszk)

    session.commit()

    zabojstwo_xlsx = os.path.join(folder_path, 'filtered_przestepstwa_Zabójstwo.xlsx')
    zabojstwo_df = pd.read_excel(zabojstwo_xlsx)

    for index, row in zabojstwo_df.iterrows():
        zab = Zabojstwa(id_ob=int(row['Jednostka organizacyjna Policji']), przestepstwa_wyk_zab=int(row['Postępowania wszczęte']), rok_zab=int(row['Rok']))
        session.add(zab)

    session.commit()

    zgwalcenia_xlsx = os.path.join(folder_path, 'filtered_przestepstwa_Zabójstwo.xlsx')
    zgwalcenia_df = pd.read_excel(zgwalcenia_xlsx)

    for index, row in zgwalcenia_df.iterrows():
        zgw = Zgwalcenia(id_ob=int(row['Jednostka organizacyjna Policji']), przestepstwa_wyk_gwalt=int(row['Postępowania wszczęte']), rok_gwalt=int(row['Rok']))
        session.add(zgw)

    session.commit()

    ogolem_xlsx = os.path.join(folder_path, 'filtered_przestepstwa_Ogólnie.xlsx')
    ogolem_df = pd.read_excel(ogolem_xlsx)

    for index, row in ogolem_df.iterrows():
        og = Przestepstwa(id_ob=int(row['Jednostka podziału administracyjnego']), przestepstwo_stw=int(row['Przestępstwa stwierdzone']), przestepstwo_wyk=int(row['Przestępstwa wykryte']), rok=int(row['Rok']))
        session.add(og)

    session.commit()

    
    
    

load_data()

session.close()


