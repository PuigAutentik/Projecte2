from pymongo import MongoClient
import pandas as pd
import math
import openpyxl
from datetime import datetime
from datetime import *
import datetime

MONGODB_URI = 'mongodb+srv://admin:admin@tpuig.rrxjzrd.mongodb.net/?retryWrites=true&w=majority'

client= MongoClient(MONGODB_URI)

df = pd.read_excel("Tasca3.xlsx")

df_to_dict = df.to_dict('records')

# Create the database 'hospital'
db = client['hospital']

# Create the collection 'usuaris'
usuaris = db['usuaris']
pacients = db['pacients']
metges =db['metges']

usuaris.delete_many({})
pacients.delete_many({})
metges.delete_many({})

df_dos = df.isna()

df_true_false = df_dos.to_dict('records')

inserir = list()

i=0
for row in df_to_dict:
    prov = dict()
    for k in row:
        if df_true_false[i][k]:
            print(k, '=>' ,'NO ENTRA AL DICCIONARI')
        else:
            prov[k]=row[k]
            #print(k, '=>' ,row[k])
        if k=='Mutua':
            print('mutua')
        elif k=='Especialitat':
            print('Es metge')
    print()
    inserir.append(prov)
    i=i+1


for user in inserir:
    result=usuaris.insert_one(user)
    id = result.inserted_id

    if user.get('Mutua')!=None:
        documento = {
            "_id": id,
            "id_temporal": user.get("id_temporal"),
            "nom_mutua": user.get("Mutua"),
            "num_mutualista": user.get("Num_mutualista")
        }
        pacients.insert_one(documento)
    
    if user.get('Especialitat')!=None:
        documento = {
            "_id": id,
            "id_temporal": user.get("id_temporal"),            
            "nom_mutua": user.get("Especialitat"),
            "num_mutualista": user.get("Num_colegiat")
        }
        metges.insert_one(documento)
    print(id)
    print()

############################# AGENDA AFEGIDA ALS MATGES ####################################

df3 = pd.read_excel("Tasca3.xlsx",sheet_name='HORARIS')

dicionari_horaris = df3.to_dict('records')

horaris = list()

for horari in dicionari_horaris:
    prov = dict()
    for k in horari:
        if isinstance(horari[k],float):
            print()
        else:
            prov[k]=horari[k]
            #print(k,' => ',horari[k])
    horaris.append(prov)

##### PASAR DIAS DE LA SETMANA A 0,1,2,3
dias_semana = {'Dilluns': 0, 'Dimarts': 1, 'Dimecres': 2, 'Dijous': 3, 'Divendres': 4, 'Dissabte': 5}

for diccionario in horaris:
    for clave, valor in diccionario.items():
        if clave in dias_semana and valor == 's':
            # reemplaza el valor 's' con el valor numérico correspondiente
            diccionario[clave] = dias_semana[clave]


####### RECORRER CADA METGES I FER EL SEU HORARI I INTRODUIRLO

for x in horaris:
    dias_treballa = []

    for clave, valor in x.items():
        if clave == 'Dilluns' or clave == 'Dimarts' or clave == 'Dimecres' or clave == 'Dijous' or clave == 'Divendres' or clave == 'Dissabte':
            dias_treballa.append(valor)

    hora_str = x['Inici Horari'].strftime('%H:%M:%S')
    hora_parts_inici = hora_str.split(':')

    hora_str = x['Fi Horari'].strftime('%H:%M:%S')
    hora_parts_final = hora_str.split(':')

    hora_inicio = datetime.time(int(hora_parts_inici[0]),int(hora_parts_inici[1]))

    hora_fin = datetime.time(int(hora_parts_final[0])-1,30)

    intervalo = datetime.timedelta(minutes=30)

    llista = list()    

    inici = datetime.date(2023, 1, 1)
    seis_meses_despues = inici + datetime.timedelta(days=180)
    rango_fechas = [inici + datetime.timedelta(days=dia) for dia in range(180)]

    for fecha in rango_fechas:
        if fecha.weekday() in dias_treballa:
            # Combinar la fecha actual con la hora de inicio para crear la hora actual
            hora_actual = datetime.datetime.combine(fecha, hora_inicio)

            while hora_actual.time() <= hora_fin:
                diccionario = {}
                clave = hora_actual.strftime("%Y-%m-%d %H:%M:%S")

                diccionario['moment_visita']=clave
                 #diccionario[clave] = {}
                llista.append(diccionario)
                hora_actual += intervalo

    ##INTRODUIR DADES
    metges.update_one({'id_temporal': x['id_temporal_metge']}, {'$push': {'agenda': {'$each': llista}}})

############################# USUSARIS, METGES I PACIENTS AMB LES RELACIONS CORRESPONENTS CREATS CORRECTAMENT ####################################


df2 = pd.read_excel("Tasca3.xlsx",sheet_name='VISITES')

dicionari_visites = df2.to_dict('records')

#metges.update_many({}, { "$unset": { "agenda": "" } })

for visita in dicionari_visites:
    resultat = usuaris.find_one({'id_temporal': visita.get('id_temporal_pacient')}, {'_id': 1})
    if resultat==None:
        medico = {
            "agenda": [{
                "moment_visita": visita.get('Moment_visita'),
                "realitzada": visita.get('Realitzada'),
                "informe": visita.get('Informe')
            }]
        }
    else:
        medico = {
            "agenda": [{
                "moment_visita":visita.get('Moment_visita'),
                "pacient": resultat.get('_id'),
                "realitzada": visita.get('Realitzada'),
                "informe": visita.get('Informe')
            }]
        }
    
    #metges.update_one({'id_temporal': visita.get('id_temporal_metge')}, {"$addToSet": {"agenda": {"$each": medico["agenda"]}}}, upsert=True)
    #metges.update_one({'id_temporal': visita.get('id_temporal_metge'), "agenda.moment_visita": visita.get('Moment_visita')}, {"$set": {"agenda.$": medico['agenda'][0]}}, upsert=True)
    metge = metges.find_one({'id_temporal': visita.get('id_temporal_metge')})
    if metge:
        agenda = metge.get('agenda', [])
        moment_visita = visita.get('Moment_visita')
        moment_visita_bd = datetime.datetime.strptime(moment_visita, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
        index = next((index for (index, d) in enumerate(agenda) if d["moment_visita"] == moment_visita_bd), None)
        if index is not None:
            agenda[index] = medico['agenda'][0]
        else:
            agenda.append(medico['agenda'][0])
        metges.update_one({'id_temporal': visita.get('id_temporal_metge')}, {"$set": {'agenda': agenda}})
    else:
        metges.insert_one(medico)

########## BORRAR COSES #####################################################################

usuaris.update_many({}, {"$unset": {"Mutua": 1, "Num_mutualista": 1, "Especialitat": 1, "Num_colegiat": 1, "id_temporal": 1}})
pacients.update_many({}, {"$unset": {"id_temporal": ""}})
metges.update_many({}, {"$unset": {"id_temporal": ""}})
metges.update_many({}, {'$rename': {"nom_mutua": "especialitat", "num_mutualista": "num_colegiat"}})
usuaris.update_many({}, { '$rename':{ "Cognoms,_i_Nom": "Cognoms_i_Nom" } })


