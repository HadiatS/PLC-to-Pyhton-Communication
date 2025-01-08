import os
import time
import csv
import logging
from datetime import datetime
from pymcprotocol import Type3E
from datetime import datetime
import threading

# Konfigurasi Koneksi ke PLC
PLC_IP = "192.168.0.1"
PLC_PORT = 1025
REGISTER_COUNT = 20  # Total register di PLC
BASE_FOLDER = r"D:\Nuspar\01.Generated_SPK"  #r"D:\testAveva\01.DummyERP"
FEEDBACK_FOLDER = r"D:\Nuspar\03.AfterProgress" #r"D:\testAveva\03.FeedbackFromAveva"
GET_AVEVA_FOLDER = r"D:\Nuspar\02.Progress" #r"D:\testAveva\02.GetAveva"
LOG_FOLDER = r"D:\Nuspar\logs"
HISTORY_FILE = r"D:\Nuspar\logs\history.csv"
#data jika tidak ada drive C
#BASE_FOLDER = r"C:\Users\user\Documents\01.Generated_SPK"
#FEEDBACK_FOLDER = r"C:\Users\user\Documents\03.AfterProgress"
#GET_AVEVA_FOLDER = r"C:\Users\user\Documents\02.Progress" 
#LOG_FOLDER = r"C:\Users\user\Documents\logs"
#HISTORY_FILE = r"C:\Users\user\Documents\logs\history.csv"

log_filename = datetime.now().strftime('%Y-%m-%d') + '_Nuspar_Connection_Log.txt'

LOG_FILE = os.path.join(LOG_FOLDER, log_filename)

if not os.path.exists(LOG_FOLDER):
    os.makedirs(LOG_FOLDER)

#setup logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(LOG_FILE,mode='a'),
                        logging.StreamHandler()
                        ]
                    )

# Inisialisasi Koneksi PLC
plc = Type3E()
plc.soc_timeout = 10

# Tracking register PLC untuk menghindari pengiriman ulang
plc_registers = {}

# Mapping untuk Item Material dan Production Storage
ITEM_MATERIAL_MAPPING = {"M500": 1, "M518": 2, "H3PO4": 3}
PRODUCTION_STORAGE_MAPPING = {"RT1": 1, "RT2": 2, "RT3": 3, "RT4": 4, "RT4a": 5, "RT5": 6, "RT6": 7, "RT7": 8, "RT8": 9, "Spare": 10}

#membuat file csv baru jika terjadi masalah
def get_new_history_filename(base_filename):
    """Menghasilkan nama file baru dengan urutan jika file tidak dapat diakses."""
    base_name, ext = os.path.splitext(base_filename)
    counter = 1
    while True:
        new_filename = f"{base_name}_{counter}{ext}"
        if not os.path.exists(new_filename):
            return new_filename
        counter += 1
#tulis ke csv
def write_to_csv(file_data, base_filename):
    """Menulis data ke file CSV, memastikan hanya satu file baru dibuat jika file utama tidak bisa diakses."""
    history_file = get_valid_history_file(base_filename)

    try:
        with open(history_file, 'a', newline='') as file:
            fieldnames = ["Tanggal SPK", "SPK No", "Item Material", "kebutuhan", "production storage", "user id", "status", "Finish Read Time"]
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            # Tambahkan header jika file kosong
            if os.stat(history_file).st_size == 0:
                writer.writeheader()

            # Tulis data ke file
            writer.writerow(file_data)
            print(f"Data berhasil ditulis ke {history_file}: {file_data}")
    except (PermissionError, OSError) as e:
        print(f"Gagal mengakses {history_file}. Error: {e}")


#get history file 
def get_valid_history_file(base_filename):
    """Mengembalikan file history yang valid untuk ditulis."""
    default_file = base_filename
    backup_file = base_filename.replace(".csv", "_1.csv")

    # Coba gunakan file utama
    if os.access(default_file, os.W_OK) or not os.path.exists(default_file):
        return default_file

    # Jika file utama tidak bisa diakses, gunakan backup
    if os.access(backup_file, os.W_OK) or not os.path.exists(backup_file):
        return backup_file

    # Jika backup juga tidak bisa diakses, buat backup tambahan
    counter = 2
    while True:
        new_backup = base_filename.replace(".csv", f"_{counter}.csv")
        if not os.path.exists(new_backup):
            return new_backup
        if os.access(new_backup, os.W_OK):
            return new_backup
        counter += 1

       
# Pastikan Folder Ada
def ensure_folders_exist():
    """Memastikan semua folder yang dibutuhkan ada."""
    for folder in [BASE_FOLDER, FEEDBACK_FOLDER, GET_AVEVA_FOLDER,LOG_FOLDER]:
        if not os.path.exists(folder):
            os.makedirs(folder)
    print("All necessary folders are ready.")
    logging.info("All necessary folders are ready.")

# Koneksi dengan Timeout
def connect_with_timeout():
    """Menghubungkan ke PLC dengan retry jika gagal."""
    for attempt in range(5):  # Maksimal 5 percobaan
        try:
            plc.connect(PLC_IP, PLC_PORT)
            #plc.batchread_wordunits("D1200", 1)  # Dummy read untuk memastikan koneksi berhasil
            print("Connection successful.Menunggu File SPK")
            logging.info("Koneksi OK. menunggu File SPK")
            return True
        except Exception as e:
            print(f"Attempt {attempt + 1}/5: Connection error: {e}")
            logging.warning(f"Attempt {attempt + 1}/5: Connection error: {e}")
        time.sleep(1)
    print("Failed to connect to PLC after multiple attempts.")
    logging.warning("Failed to connect to PLC after multiple attempts.")
    return False

# Load register dari PLC

def load_registers_from_plc():
    """Muat data dari semua register PLC."""
    global plc_registers
    try:
        for i in range(REGISTER_COUNT):
            base_register = 1200 + i * 20
            #ganti dari 1200 ke 1600
            #base_register = 1600 + i * 20
            spk_no = plc.batchread_wordunits(f"D{base_register}", 1)[0]
            item_material = plc.batchread_wordunits(f"D{base_register + 2}", 1)[0]
            kebutuhan = plc.batchread_wordunits(f"D{base_register + 3}", 1)[0]
            production_storage = plc.batchread_wordunits(f"D{base_register + 4}", 1)[0]
            user_id = plc.batchread_wordunits(f"D{base_register + 5}", 1)[0]

            if spk_no != 0:  # Register tidak kosong
                plc_registers[i] = {
                    "SPK No": spk_no,
                    "Item Material": item_material,
                    "kebutuhan": kebutuhan,
                    "production storage": production_storage,
                    "user id": user_id,
                }
            else:
                # Jika register kosong (spk_no == 0), hapus dari plc_registers
                if i in plc_registers:
                    del plc_registers[i]
    except Exception as e:
        print(f"Error saat membaca register dari PLC: {e}")
        logging.warning(f"Error saat membaca register dari PLC: {e}")
        
# Baca file teks dari folder
def read_txt_files(folder):
    """Baca file .txt dari folder tertentu."""
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    return sorted(files, key=os.path.getmtime)

# Parse data dari file teks
def parse_file(file_path):
    """Parse data dari file berdasarkan urutan kolom tetap."""
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()

        if len(lines) < 2:
            print(f"Error: File {file_path} tidak memiliki cukup baris untuk diproses.")
            logging.warning(f"Error: File {file_path} tidak memiliki cukup baris untuk diproses.")
            return {}

        data = lines[1].strip().split()  # Pisahkan berdasarkan spasi atau tab

        if len(data) < 6:
            print(f"Error: Format file {file_path} tidak sesuai (kolom tidak lengkap).")
            logging.warning(f"Error: Format file {file_path} tidak sesuai (kolom tidak lengkap).")
            return {}

        item_material_name = data[2]
        item_material = ITEM_MATERIAL_MAPPING.get(item_material_name.upper(), 0)
        production_storage_name = data[4]
        production_storage = PRODUCTION_STORAGE_MAPPING.get(production_storage_name.upper(), 0)

        parsed_data = {
            "SPK No": int(data[1]),
            "Item Material": item_material,
            "kebutuhan": int(data[3]),
            "production storage": production_storage,
            "user id": int(data[5]),
        }

        return parsed_data
    except Exception as e:
        print(f"Error: Tidak dapat memproses file {file_path}. Detail: {e}")
        return {}

# Hitung total kebutuhan
def calculate_total_kebutuhan(file_data):
    """Menghitung total kebutuhan berdasarkan data baru, register PLC, dan history.csv."""
    total = file_data["kebutuhan"]  # Mulai dengan kebutuhan dari file baru

    # Tambahkan kebutuhan dari register PLC yang cocok
    for register_idx, data in plc_registers.items():
        if (
            data["SPK No"] == file_data["SPK No"]
            and data["Item Material"] == file_data["Item Material"]
            and data["production storage"] == file_data["production storage"]
            and data["user id"] == file_data["user id"]
        ):
            total += data["kebutuhan"]
    return total


# Tulis data ke PLC
def write_to_plc(registers, data):
    """Tulis data ke PLC pada register tertentu."""
    try:
        for reg, value in zip(registers, data):
            plc.batchwrite_wordunits(headdevice=reg, values=[int(value)])
        print(f"Data berhasil ditulis ke PLC: {data}")
        logging.error(f"Data Berhasil ditulis ke PLC: {data}")
    except Exception as e:
        print(f"Error saat menulis data ke PLC: {e}")
        logging.error(f"Error saat send data to PLC: {e}")
#cari file txt di history
def find_matching_txt_file(spk_no, item_material, kebutuhan, production_storage, user_id):
    """Cari file TXT yang cocok dengan data dari register PLC."""
    for file in os.listdir(GET_AVEVA_FOLDER):
        if file.endswith(".txt"):
            file_path = os.path.join(GET_AVEVA_FOLDER, file)
            file_data = parse_file(file_path)
            if (
                file_data.get("SPK No") == spk_no
                and file_data.get("Item Material") == item_material
                and file_data.get("kebutuhan") == kebutuhan
                and file_data.get("production storage") == production_storage
                and file_data.get("user id") == user_id
            ):
                return file_path
    return None


# Proses M1200 -> rubah ke M1600 [15 Dec 2024]
def handle_finish_read(register_idx):
    """Menangani register yang selesai diproses berdasarkan sinyal M12xx dan mengubah status menjadi 'Finish Read'."""
    base_register = 1200 + register_idx * 20
    finish_bit =f"M{1600+ register_idx}"
    reset_bit = f"M{1650 + register_idx}"
    try:
        logging.info(f"Mulai membaca data dari register {register_idx}.")
        spk_no = plc.batchread_wordunits(f"D{base_register}", 1)[0]
        item_material = plc.batchread_wordunits(f"D{base_register + 2}", 1)[0]
        kebutuhan = plc.batchread_wordunits(f"D{base_register + 3}", 1)[0]
        production_storage = plc.batchread_wordunits(f"D{base_register + 4}", 1)[0]
        user_id = plc.batchread_wordunits(f"D{base_register + 5}", 1)[0]
        logging.info(f"Data dari register {register_idx}: SPK No: {spk_no}, Item Material: {item_material}, "
                     f"Kebutuhan: {kebutuhan}, Production Storage: {production_storage}, User ID: {user_id}.")
    except Exception as e:
        logging.error(f"Error membaca data dari register {register_idx}: {e}")
        return

    # Cari file TXT yang sesuai di folder GetAveva
    matching_file = find_matching_txt_file(spk_no, item_material, kebutuhan, production_storage, user_id)
    if matching_file:
        logging.info(f"File yang cocok ditemukan: {matching_file}")
        # Ambil tanggal SPK dari nama file
        tanggal_spk = get_tanggal_spk_from_filename(matching_file)

        # Menunggu sinyal dari M12xx untuk mengubah status menjadi Finish Read
        #logging.info(f"Menunggu sinyal dari PLC (M{1200 + register_idx}) untuk mengubah status file menjadi Finish Read...")
        logging.info(f"Menunggu sinyal dari PLC (M{1600 + register_idx}) untuk mengubah status file menjadi Finish Read...")
        #while plc.batchread_bitunits(f"M{1200 + register_idx}", 1)[0] != 1:
        while plc.batchread_bitunits(f"M{1600 + register_idx}", 1)[0] != 1:
            time.sleep(0.1)

        # Setelah mendapatkan sinyal, catat waktu Finish Read dan ubah status menjadi Finish Read
        #update statys file
        update_file_status(matching_file, "Finish Read", FEEDBACK_FOLDER) #change dari Get_aveva to Feedback_folder
        logging.info(f"Status file TXT {matching_file} diperbarui menjadi 'Finish Read'.")
        finish_read_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history_data = {
            "Tanggal SPK": tanggal_spk,
            "SPK No": spk_no,
            "Item Material": item_material,
            "kebutuhan": kebutuhan,
            "production storage": production_storage,
            "user id": user_id,
            "status": "Finish Read",
            "Finish Read Time": finish_read_time,
        }
        write_to_csv(history_data, HISTORY_FILE)
        logging.info(f"Data telah dicatat dalam CSV: {history_data}")    
        logging.info(f"Mereset register di PLC (M{1650 + register_idx})")
        reset_register(register_idx)
        plc.batchwrite_bitunits(headdevice=reset_bit, values=[1])
        time.sleep(0.1)
        plc.batchwrite_bitunits(headdevice=reset_bit, values=[0])
        #logging.info(f"Reset register (M{1300 + register_idx}) selesai.")
        logging.info(f"Reset register (M{1650 + register_idx}) selesai.")
        time.sleep(2)

    else:
        spk_no = plc.batchread_wordunits(f"D{base_register}", 1)[0]

        if spk_no == 0:
            # Jika SPK No sudah reset dan tidak ada file, reset status register dan set bit reset
            logging.info(f"Data register sudah direset, tidak ditemukan file yang sesuai. Reset status register {register_idx}.")
            reset_register(register_idx)
            plc.batchwrite_bitunits(headdevice=reset_bit, values=[1])
            time.sleep(0.1)
            plc.batchwrite_bitunits(headdevice=reset_bit, values=[0])
            logging.info(f"Register {register_idx} direset ke 0 setelah status menjadi Finish Read.")
            time.sleep(2)
        else:
            # Jika file tidak ditemukan dan register belum reset, lanjutkan tanpa perubahan
            logging.warning(f"Tidak ditemukan file TXT yang sesuai untuk register {register_idx}. Data masih di register.")
            time.sleep(0.5)
#reset register        
def reset_register(register_idx):
    """Reset register PLC ke 0 setelah status file menjadi Finish Read."""
    base_register = 1200 + register_idx * 20
    try:
        # Menulis nilai 0 ke register yang relevan
        plc.batchwrite_wordunits(f"D{base_register}", [0])           # Reset SPK No
        plc.batchwrite_wordunits(f"D{base_register + 2}", [0])       # Reset Item Material
        plc.batchwrite_wordunits(f"D{base_register + 3}", [0])       # Reset Kebutuhan
        plc.batchwrite_wordunits(f"D{base_register + 4}", [0])       # Reset Production Storage
        plc.batchwrite_wordunits(f"D{base_register + 5}", [0])       # Reset User ID

        # Setelah reset, hapus data terkait dari plc_registers
        if register_idx in plc_registers:
            del plc_registers[register_idx]
        print(f"Register {base_register} direset ke 0 setelah status menjadi Finish Read.")
    except Exception as e:
        print(f"Error saat mereset register {register_idx}: {e}")


 #dapatkan tanggal SPK       
def get_tanggal_spk_from_filename(file_path):
    """Mengambil Tanggal SPK dari nama file."""
    try:
        file_name = os.path.basename(file_path)
        tanggal_spk = file_name.split("_")[-1].replace(".txt", "")
        return tanggal_spk
    except Exception as e:
        print(f"Error saat mengambil Tanggal SPK dari nama file {file_path}: {e}")
        return "Unknown"


# Proses file ke register
def handle_file(file_data, file_path, register_idx):
    """Kirim file ke register PLC tanpa mengubah status menjadi 'Finish Read'."""
    base_register = 1200 + register_idx * 20
    registers = [
        f"D{base_register}",
        f"D{base_register + 2}",
        f"D{base_register + 3}",
        f"D{base_register + 4}",
        f"D{base_register + 5}",
    ]

    # Hitung total kebutuhan
    total_kebutuhan = calculate_total_kebutuhan(file_data)

    # Validasi kebutuhan > 2000
    if total_kebutuhan > 2000:
        print(f"Kebutuhan > 2000 untuk SPK No: {file_data['SPK No']} (Total: {total_kebutuhan}). Mengaktifkan M261.")
        
        # Kirim sinyal M261 ke PLC untuk menandakan bahwa data sedang diproses
        plc.batchwrite_bitunits(headdevice="M261", values=[1])
        
        # Pindahkan data ke register D1600 (karena kebutuhan lebih dari 2000)
        move_data_to_register(file_data, 1600)

        # Menunggu konfirmasi dari PLC pada M262
        while plc.batchread_bitunits("M262", 1)[0] != 1:
            print("Menunggu konfirmasi dari PLC pada M262...")
            time.sleep(0.1)
        
        # Reset sinyal M261 setelah konfirmasi dari PLC
        plc.batchwrite_bitunits(headdevice="M261", values=[0])
        print("M261 telah di-reset setelah konfirmasi dari M262.")
                 
        # Pindahkan file ke folder Feedback Aveva dengan status Cancelled
        update_file_status(file_path, "Cancelled", FEEDBACK_FOLDER)
        print(f"File {file_path} dipindahkan ke folder Feedback Aveva dengan status 'Cancelled'.")
                # Reset register D1600 setelah menerima konfirmasi
        # Menulis nilai 0 ke register yang relevan
        plc.batchwrite_wordunits("D1600", [0])           # Reset SPK No
        plc.batchwrite_wordunits("D1602", [0])       # Reset Item Material
        plc.batchwrite_wordunits("D1603", [0])       # Reset Kebutuhan
        plc.batchwrite_wordunits("D1604", [0])       # Reset Production Storage
        plc.batchwrite_wordunits("D1605", [0])       # Reset User ID
        
    else:
        # Untuk kebutuhan <= 2000, proses file seperti biasa
        # Kirim data ke PLC pada register yang sesuai
        write_to_plc(registers, [
            file_data["SPK No"],
            file_data["Item Material"],
            file_data["kebutuhan"],
            file_data["production storage"],
            file_data["user id"],
        ])

        # Pindahkan file ke folder GetAveva dan status menjadi 'Pending'
        update_file_status(file_path, "Pending", GET_AVEVA_FOLDER)
        print(f"File {file_path} dipindahkan ke folder GetAveva dan status diubah menjadi 'Pending'.")

    # Tambahkan ke tracking (baik yang >2000 maupun yang <=2000)
    plc_registers[register_idx] = file_data

def move_data_to_register(file_data, base_register):
    """Pindahkan data file ke register PLC (misalnya D1600, D1602, D1603, dll)."""
    registers = [
        f"D{base_register}",
        f"D{base_register + 2}",
        f"D{base_register + 3}",
        f"D{base_register + 4}",
        f"D{base_register + 5}",
    ]
    write_to_plc(registers, [
        file_data["SPK No"],
        file_data["Item Material"],
        file_data["kebutuhan"],
        file_data["production storage"],
        file_data["user id"],
    ])
    print(f"Data untuk SPK No: {file_data['SPK No']} dipindahkan ke register mulai dari D{base_register}.")



# Update status file
def update_file_status(file_path, new_status, target_folder):
    """Ubah status dalam file TXT dan pindahkan ke folder tujuan."""
    try:
        logging.info(f"Mulai proses update status file {file_path} ke '{new_status}'")
        # Baca konten file
        with open(file_path, 'r') as file:
            lines = file.readlines()

        # Ubah status pada baris dengan header "Status"
        updated_lines = []
        status_updated = False
        for line in lines:
            if line.strip().startswith("Status"):
                parts = line.split()
                # Ubah kolom terakhir menjadi status baru
                parts[-1] = new_status
                updated_lines.append(" ".join(parts) + " ")
                status_updated = True
            else:
                updated_lines.append(line)

        # Tambahkan status baru jika tidak ada baris "Status" sebelumnya
        if not status_updated:
            #updated_lines.append(f"Status: {new_status}\n")
            updated_lines.append(f"{new_status}\n")

        # Tulis kembali ke file
        with open(file_path, 'w') as file:
            file.writelines(updated_lines)

        # Pindahkan file ke folder tujuan
        new_path = os.path.join(target_folder, os.path.basename(file_path))
        if os.path.exists(new_path):  # Hapus jika sudah ada file dengan nama yang sama
            os.remove(new_path)
        os.rename(file_path, new_path)

        logging.info(f"File {file_path} berhasil dipindahkan ke {new_path} dengan status {new_status}.")
    except Exception as e:
        logging.error(f"Error saat memproses file {file_path}: {e}")

# Fungsi untuk mengirim sinyal M260 dengan interval
def m260_handler(plc, interval=10):
    last_m260_time = 0
    while True:
        current_time = time.time()
        if current_time - last_m260_time >= interval:
            try:
                plc.batchwrite_bitunits(headdevice="M260", values=[1])
                print(f"M260 dikirim dengan nilai 1 ke PLC (Interval: {interval} detik).")
                logging.info("Hearbeart Success")
                last_m260_time = current_time
            except Exception as e:
                logging.error(f"Error saat mengirim M260: {e}")
        time.sleep(1)  # Hindari polling terus-menerus

# Fungsi utama
def main():
    ensure_folders_exist()

    if not connect_with_timeout():
        return

    # Load register dari PLC
    load_registers_from_plc()

    #connection to PLC
    # Jalankan thread untuk mengelola M260
    m260_thread = threading.Thread(target=m260_handler, args=(plc, 2), daemon=True)
    m260_thread.start()

    while True:
        # Periksa M1200 untuk semua register ganti menjadi M1600 dan M1650
        for idx in range(REGISTER_COUNT):
            #finish_bit = f"M{1200 + idx}"
            #reset_bit = f"M{1300 + idx}"
            finish_bit = f"M{1600 + idx}"
            reset_bit = f"M{1650 + idx}"
            if plc.batchread_bitunits(finish_bit, 1)[0] == 1:
                handle_finish_read(idx)

        # Cari register kosong (termasuk yang sudah direset ke 0)
        empty_registers = [i for i in range(REGISTER_COUNT) if i not in plc_registers]

        if not empty_registers:
            print("Semua register penuh. Menunggu register kosong...")
            time.sleep(1)
            continue

        # Cari register kosong secara berurutan (dari yang pertama hingga yang terakhir)
        for register_idx in range(REGISTER_COUNT):
            if register_idx not in plc_registers:
                # Jika register kosong, proses file dan kirim data ke register kosong ini
                #print(f"Register {register_idx} kosong, memproses file untuk mengisi register.")
                # Ambil file dari folder dan proses
                files = read_txt_files(BASE_FOLDER)
                for file in files:
                    file_data = parse_file(file)
                    if not file_data:
                        continue

                    # Kirim data ke register kosong
                    handle_file(file_data, file, register_idx)
                    print(f"File {file} diproses di register {register_idx}.")
                    
                    # Setelah mengisi register, kita keluar dari loop file karena satu file hanya bisa mengisi satu register
                    break

        time.sleep(0.5)

if __name__ == "__main__":
    main()
