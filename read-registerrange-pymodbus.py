from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.client.sync import  ModbusSerialClient
from pymodbus.constants import Endian
import datetime, time, os

### Spechergerödel
if not os.path.exists('data'):
    os.makedirs('data')
csvpath = "data\data.csv"

rows_per_file = 7200 # after 60 lines we write into another file
max_files = 1000 # we stop after making those number of files
every = 0.1 # Nur alle 200ms neue Daten
counter = 0 # counts the rows and resets for a new file
i = 1 # counts the files

### Modbus Gerödel
register = [19000, 19002, 19004, 19012, 19014, 19016, 19018,
            19020, 19022, 19024, 19026, 19028,
            19030, 19032, 19034, 19036, 19038,
            19040, 19042,
            19062, 19064, 19066, 19068,
            19070, 19072, 19074, 19076, 19078,
            19080, 19082, 19084,
            19092, 19094, 19096,19098,
            19100, 19102, 19104, 19106, 19108]
registernr = min(register) # Zählervariable um Daten zuzuordnen

### Modbus Verbindungsparameter
client = ModbusSerialClient(method='rtu',port='COM8', timeout=1, parity='N', baudrate=115200, unit=1)
client.connect()

### Funktionen Modbus
# Werte aus den Registern in Float umwandeln
def createvalue(data):
    decoder = BinaryPayloadDecoder.fromRegisters(data, Endian.Big, wordorder=Endian.Big)
    value = decoder.decode_32bit_float()
    return value

# Regisgterrange auslesen definiert sich durch die höchste und niedrigeste Zahl der register Liste
def readregisterrange():
    # Zeitstempel erzeugen
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") ; now = time.time() # War mal datetime.datetime.utcnow()
    
    add_min, add_max = min(register), max(register)
    no_bytes = (add_max - add_min) + 2 # +2 damit auch das letzte Register nicht fehlt
    response = client.read_holding_registers(add_min, no_bytes, unit=0x01)
    return response.registers, timestamp, now

# Dictonary erstellen bestehend aus dem ersten Register als Key und den Werten der 2 Register als Liste
def createregisterdict(response, registernr):
    registerdict = {}
    for i in range(0, int((len(response)/2))):
        registerdict.update({registernr:[response.pop(0), response.pop(0)]}) # Key erstellen mit 2 Values 
        # print("Register NR:" + str(registernr) + ": " + str(data) + " = " + str(createvalue(data))) # DEV Output
        registernr = registernr + 2 # Registernr in 2er Schritten hochzählen
    return registerdict

### Funktionen Speichern
# Zeile in CSV schreiben
def writerow(csvpath, row):
    with open(csvpath, "a") as csvfile:# we write row by row and element by element; "a" for append 
        for y in range(len(row)):
            if y == len(row)-1: # to make a new line at the end instead of writing a ";"
                csvfile.write(str(row[y]) + "\n")
            else:
                csvfile.write(str(row[y]) + ";")
                
### Hauptprogramm 
while True:
    # Zeitstempel wird in Auslesefunktion und nicht hier erzeugt um möglichst richtig zu sein
    readregister, timestamp, now = readregisterrange()
    
    # Ganze Registerrange in ein Dictonary legen
    data = createregisterdict(readregister,registernr)
    
    # Speicherdaten erstellen und mit Timestamp verheiraten
    row = []
    row.append(timestamp)
    
    # Nur die Items aus dem Dictonary umrechenn die uns interessieren    
    for item in register:
        # print(str(item) + " : " + str(createvalue(data[item]))) # Dev Output
        row.append(createvalue(data[item]))
        
    # Schreiben und Zählen der Zeilen 
    writerow(csvpath,row) # Writes the row into the datafile
    counter += 1 # we count the rows
    if counter == rows_per_file: # to change the file we write into
        i += 1
        counter = 0
        csvpath = "data\data" + str(i) +".csv"
    if i == max_files: # to
        break
    
    # Nur alle 0.2 s wieder ausführen
    delta = time.time()-now
    time.sleep(every-delta)