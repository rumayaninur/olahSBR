import requests
import pandas as pd
import time
import sys
import json
import re
from login import login_with_sso, user_agents

version = "1.2.6"
motd = 1

def main():
    # Pengecekan versi
    try:
        response = requests.get("https://dev.ketut.web.id/ver.txt", timeout=10)
        if response.status_code == 200:
            remote_version = response.text.strip()
            if remote_version != version:
                print(f"Versi saat ini: {version}")
                print(f"Versi terbaru: {remote_version}")
                print("Gunakan versi terbaru. Silakan unduh dari:")
                print("https://github.com/ketut/SsscriptGC")
                time.sleep(5)
                sys.exit(1)
        else:
            print("Gagal mengambil versi terbaru. Melanjutkan...")
    except Exception as e:
        print(f"Gagal mengecek versi: {e}. Melanjutkan...")

    # Cek MOTD dan tampilkan pesan jika motd = 1
    try:
        motd_response = requests.get("https://dev.ketut.web.id/TGlrZWxpaG9vZA.txt", timeout=10)
        if motd_response.status_code == 200:
            motd_text = motd_response.text.strip()
            print(f"{motd_text}")  # Debug
            try:
                motd_data = motd_response.json()
                if motd_data.get("motd") == 1:
                    print(motd_data.get("message", ""))
            except:
                # Jika bukan JSON, asumsikan teks adalah motd
                if motd_text == "1":
                    print("Pesan MOTD aktif")  # Placeholder, ganti dengan pesan sebenarnya jika ada
    except Exception as e:
        print(f"[DEBUG] Gagal cek MOTD: {e}")

    if len(sys.argv) < 3:
        print("Usage: python tandaiKirim.py <username> <password> [OTP jika menggunakan OTP]")
        sys.exit(1)

    username = sys.argv[1]
    password = sys.argv[2]
    otp_code = sys.argv[3] if len(sys.argv) > 3 else None
    nomor_baris = int(sys.argv[4]) if len(sys.argv) > 4 else None

    # Jika nomor_baris tidak diberikan, baca dari baris.txt
    if nomor_baris is None:
        try:
            with open('baris.txt', 'r') as f:
                nomor_baris = int(f.read().strip())
        except FileNotFoundError:
            nomor_baris = 0

    # Lakukan login dan dapatkan objek halaman
    page, browser = login_with_sso(username, password, otp_code)

    if page:
        try:
            # DEBUG: Cek identitas browser
            ua = page.evaluate("navigator.userAgent")
            print(f"\n[INFO] Browser User Agent: {ua}")
            if "Android" not in ua and "Mobile" not in ua:
                print("⚠️  WARNING: Script tidak berjalan dalam mode Mobile!")
                print("    Kemungkinan file 'login.py' belum diupdate di laptop ini.")
            else:
                print("[INFO] Mode Mobile aktif. Melanjutkan...\n")

            # Navigasi ke /dirgc
            url_gc = "https://matchapro.web.bps.go.id/dirgc"
            page.goto(url_gc, timeout=60000)
            page.wait_for_load_state('networkidle', timeout=60000)

            # Dapatkan cookies
            cookies = page.context.cookies()
            session_cookies = {cookie['name']: cookie['value'] for cookie in cookies}

            url = "https://matchapro.web.bps.go.id/dirgc/"

            # Baca CSV
            encodings_to_try = ['utf-8', 'cp1252', 'latin1']
            df = None
            for enc in encodings_to_try:
                try:
                    df = pd.read_csv('data_gc_profiling_bahan_kirim.csv', encoding=enc)
                    print(f"Berhasil membaca dengan encoding: {enc}")
                    break
                except UnicodeDecodeError:
                    print(f"Gagal dengan encoding: {enc}, mencoba yang lain...")
                    continue
            if df is None:
                raise ValueError("Tidak bisa membaca file dengan encoding yang dicoba.")

            #input("Tekan Enter untuk lanjut...")

            for index in range(int(nomor_baris), len(df)):
                row = df.iloc[index]
                idsbr = str(row['idsbr'])
                hasilgc = row['hasilgc']

                # Pengecekan hasilgc
                if hasilgc is None or str(hasilgc).strip() == '' or hasilgc not in [99, 1, 3, 4]:
                    print(f"Pemberitahuan: hasilgc untuk baris {index} kosong atau tidak valid ({hasilgc}). Nilai yang diperbolehkan: 99, 1, 3, atau 4.")
                    choice = input("Apakah Anda ingin berhenti (y) atau lanjut ke baris berikutnya (n)? ").strip().lower()
                    if choice == 'y':
                        print("Proses dihentikan.")
                        sys.exit(0)
                    elif choice == 'n':
                        print("Melanjutkan ke baris berikutnya.")
                        continue
                    else:
                        print("Input tidak valid. Melanjutkan ke baris berikutnya.")
                        continue

                # Gunakan Playwright UI
                page.goto(url)

                page.click("#filter-chevron")
                page.wait_for_selector("#search-idsbr", state="visible")

                page.fill("#search-idsbr", "")
                page.fill("#search-idsbr", idsbr)
                page.wait_for_timeout(2000)

                page.wait_for_selector(".usaha-card-header", state="visible")
                page.click(".usaha-card-header")
                
                if page.locator(".btn-gc-edit").count() > 0:
                    page.click(".btn-gc-edit")
                    page.select_option("#tt_hasil_gc", value = str(hasilgc))
                    page.click("#save-tandai-usaha-btn")
                    
                    page.wait_for_selector(".swal2-popup", state="visible", timeout=5000)
                    ok_btn = page.locator("button.swal2-confirm.swal2-styled")
                    ok_btn.wait_for(state="visible", timeout=5000)
                    ok_btn.click()

                    print(f"Row {index}: Edited IDSBR {idsbr}")

                elif page.locator(".btn-gc-report").count() > 0:
                    page.click(".btn-gc-report")

                    page.wait_for_selector("#report_hasil_gc")
                    page.select_option("#report_hasil_gc", value = str(hasilgc))

                    page.click("#submit-report-gc-btn")

                    page.wait_for_selector(".swal2-popup", state="visible", timeout=5000)
                    confirm_btn = page.locator("button.swal2-confirm.swal2-styled")
                    confirm_btn.wait_for(state="visible", timeout=5000)
                    if confirm_btn.bounding_box() is not None:
                        confirm_btn.click()
                    else:
                        page.evaluate("document.querySelector('button.swal2-confirm.swal2-styled').click()")

                    ok_btn = page.locator("button.swal2-confirm", has_text="OK")
                    ok_btn.wait_for(state="visible")
                    ok_btn.click()

                    print(f"Row {index}: Reported IDSBR {idsbr}")

                request_success = True

                # Jika request berhasil, lanjutkan dengan pemrosesan response
                if request_success:
                    # Catat baris terakhir
                    try:
                        with open('baris.txt', 'w') as f:
                            f.write(str(index))
                    except PermissionError:
                        print(f"Warning: Tidak bisa menulis ke baris.txt untuk baris {index}")
              
                # Delay untuk menghindari rate limit
                time.sleep(32)

            print("Semua pengiriman selesai.")

        except Exception as e:
            print(f"Error: {e}")
        finally:
            input("Press Enter to cancel...")
            # Close browser
            browser.close()
    else:
        print("Login gagal, tidak dapat melanjutkan permintaan.")

if __name__ == "__main__":
    main()