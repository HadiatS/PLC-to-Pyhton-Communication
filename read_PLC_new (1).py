import os
import time
import csv
import logging
from datetime import datetime
from pymcprotocol import Type3E
import threading

# Konfigurasi Koneksi ke PLC
PLC_IP = "192.168.0.1"
PLC_PORT = 1027
REGISTER_COUNT = 9  # Total register di PLC
BASE_FOLDER = r"D:\Nuspar\01.SemiAuto"
FEEDBACK_FOLDER = r"D:\Nuspar\03.FinishSemiAuto"
LOG_FOLDER = r"D:\Nuspar\logs"
HISTORY_FILE = r"D:\Nuspar\logs\history_semiauto.csv"

log_filename = datetime.now().strftime('%Y-%m-%d') + '_Nuspar_SemiAuto_Log.txt'
LOG_FILE = os.path.join(LOG_FOLDER, log_filename)

if not os.path.exists(LOG_FOLDER):
    os.makedirs(LOG_FOLDER)

# Setup logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(LOG_FILE, mode='a'),
                        logging.StreamHandler()
                    ])

# Inisialisasi Koneksi PLC
plc = Type3E()
plc.soc_timeout = 40



# Mapping untuk Item Material dan Production Storage
ITEM_MATERIAL_MAPPING = {"M500": 1, "M518": 2, "H3PO4": 3}
PRODUCTION_STORAGE_MAPPING = {"RT1": 1, "RT2": 2, "RT3": 3, "RT4": 4, "RT5": 5, "RT6": 6, "RT7": 7, "RT8": 8}

# Pastikan Folder Ada
def ensure_folders_exist():
    """Memastikan semua folder yang dibutuhkan ada."""
    for folder in [BASE_FOLDER, FEEDBACK_FOLDER, LOG_FOLDER]:
        if not os.path.exists(folder):
            os.makedirs(folder)
    logging.info("Folder sudah siap semua.")

# Koneksi dengan Timeout
def connect_with_timeout():
    """Menghubungkan ke PLC dengan retry jika gagal."""
    for attempt in range(5):  # Maksimal 5 percobaan
        try:
            plc.connect(PLC_IP, PLC_PORT)
            #plc.batchread_wordunits("D1200", 1)  # Dummy read untuk memastikan koneksi berhasil
            print("Connection successful.Menunggu Data dari PLC")
            logging.info("Koneksi OK. menunggu Data Dari PLC")
            return True
        except Exception as e:
            print(f"Attempt {attempt + 1}/5: Connection error: {e}")
            logging.warning(f"Attempt {attempt + 1}/5: Connection error: {e}")
        time.sleep(1)
    print("Failed to connect to PLC after multiple attempts.")
    logging.warning("Failed to connect to PLC after multiple attempts.")
    return False


# Fungsi untuk menyimpan data ke file TXT
def save_data_to_file(data, folder_name, file_name):
    file_path = os.path.join(folder_name, file_name)
    with open(file_path, 'w') as file:
        file.write(data)
    logging.info(f"Data berhasil disimpan di: {file_path}")

# Fungsi untuk menangani sinyal M1700-M1708
def handle_m170x_signal():
    """Menangani sinyal M1700 hingga M1708 untuk menyimpan data dari PLC ke file TXT."""
    for i in range(REGISTER_COUNT):
        read_bit = f"M{1700 + i}"
        #print(read_bit)
        if plc.batchread_bitunits(read_bit, 1)[0] == 1:
            print(read_bit)
            base_register = 1500 + i * 10
            spk_no = plc.batchread_wordunits(f"D{base_register}", 1)[0]
            item_material = plc.batchread_wordunits(f"D{base_register + 2}", 1)[0]
            #Mapping Item
            print(item_material)
            item_material_name = {1: "M500",2: "M518",3: "H3PO4"}
            item_material_str = item_material_name.get(item_material,"Tidak_Ada")

            kebutuhan = plc.batchread_wordunits(f"D{base_register + 3}", 1)[0]
            #production_storage = plc.batchread_wordunits(f"D{base_register + 4}", 1)[0]
            production_storage = i + 1
            user_id = plc.batchread_wordunits(f"D{base_register + 5}", 1)[0]

            # Mapping production storage
            production_storage_name = {1: "RT1", 2: "RT2", 3: "RT3", 4: "RT4", 5: "RT5", 6: "RT6", 7: "RT7", 8: "RT8"}
            production_storage_str = production_storage_name.get(production_storage, "Tidak_Ada")
    
            # Menyusun data untuk file txt
            file_name = f"{datetime.now().strftime('%Y%m%d')}_{spk_no}_{item_material_str}_{kebutuhan}_{production_storage_str}_{user_id}.txt"
            #file_content = (f"SPK No  \n{spk_no}  "
            #               f"Item Material \n {item_material_str}  "
            #                f"Kebutuhan \n{kebutuhan}  "
            #                f"Production Storage \n {production_storage_str}  "
            #                f"User ID \n {user_id}")

            file_content = (f"SPK_No       Item_Material    Kebutuhan     Production_Storage             User_ID  Status\n"
                           f"{spk_no}     {item_material}   {kebutuhan}        {production_storage_str} {user_id}      "
                            )



            # Simpan data ke folder SemiAuto
            save_data_to_file(file_content, BASE_FOLDER, file_name)

def parse_file(file_path):
    """Mengambil data dari file TXT berdasarkan format yang ditentukan."""
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()

        # Pastikan file memiliki format yang diharapkan
        if len(lines) < 2:
            return None

        headers = lines[0].split()
        values = lines[1].split()

        if len(headers) != len(values):
            return None

        data = dict(zip(headers, values))
        return data
    except Exception as e:
        logging.error(f"Error saat membaca file {file_path}: {e}")
        return None
    
def write_to_csv(history_file, data):
    """Menulis data ke file CSV."""
    try:
        fieldnames = ["Tanggal SPK", "SPK_No", "Item_Material", "Kebutuhan", "Production_Storage", "User_ID", "Status", "Finish_Read_Time"]
        file_exists = os.path.exists(history_file)

        with open(history_file, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Tulis header jika file belum ada
            if not file_exists:
                writer.writeheader()

            # Tulis data
            writer.writerow(data)

        logging.info(f"Data berhasil dicatat ke CSV: {data}")
    except Exception as e:
        logging.error(f"Error saat menulis ke CSV: {e}")

# Fungsi untuk menangani sinyal M1750-M1758 (Memindahkan file ke FinishSemiAuto)
def handle_m175x_signal():
    """Menangani sinyal M1750 hingga M1758 untuk memindahkan file TXT ke FinishSemiAuto."""
    for i in range(REGISTER_COUNT):
        finish_bit = f"M{1750 + i}"
        if plc.batchread_bitunits(finish_bit, 1)[0] == 1:
            production_storage = f"RT{i + 1}"  # Menyesuaikan dengan RT1 - RT8
            folder_name = BASE_FOLDER
            target_folder = FEEDBACK_FOLDER

            for file_name in os.listdir(folder_name):
                if file_name.endswith(".txt"):
                    file_path = os.path.join(folder_name, file_name)
                    file_data = parse_file(file_path)

                    if file_data and file_data.get("Production_Storage") == production_storage:
                        # Pindahkan file ke folder tujuan
                        new_path = os.path.join(target_folder, file_name)
                        if os.path.exists(new_path):  # Hapus jika sudah ada file dengan nama yang sama
                            os.remove(new_path)
                        os.rename(file_path, new_path)

                        logging.info(f"File {file_name} dipindahkan ke {target_folder}.")

                        # Catat ke history CSV
                        history_data = {
                            "Tanggal SPK": datetime.now().strftime('%Y-%m-%d'),
                            "SPK_No": file_data.get("SPK_No"),
                            "Item_Material": file_data.get("Item_Material"),
                            "Kebutuhan": file_data.get("Kebutuhan"),
                            "Production_Storage": file_data.get("Production_Storage"),
                            "User_ID": file_data.get("User_ID"),
                            "Status": "Finish Read",
                            "Finish_Read_Time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        write_to_csv(HISTORY_FILE, history_data)

                        # Kirim sinyal M1760 untuk verifikasi ke PLC
                        plc.batchwrite_bitunits("M1760", [1])
                        time.sleep(0.1)
                        plc.batchwrite_bitunits("M1760", [0])
                        logging.info("Sinyal M1760 dikirim untuk verifikasi.")

# Fungsi utama
def main():
    ensure_folders_exist()

    if not connect_with_timeout():
        return


    while True:
        # Menangani sinyal M1700-M1708
        handle_m170x_signal()

        # Menangani sinyal M1750-M1758
        handle_m175x_signal()

        time.sleep(0.5)

if __name__ == "__main__":
    main()
