import pandas as pd
import numpy as np
import sys

# ganti nama u
PETUGAS_GC = 'Maya'
USERNAME = 'rumayaninur'

# ganti False kalo u mau overwrite data yg dah u gc
DROP_ALL_USERNAME = True

# sesuaikan path file hasil gc
HASIL_GC = 'Alokasi GC 7601 fix.xlsx'
SHEETS = ['Banggae', 'Bangtim', 'Pamboang', 'Tammerodo', 'Sendana', 'Tubo', 'Malunda', 'Ulumanda']

# sesuaikan path file yg akan dikirim ke matchapro
BAHAN_KIRIM = 'data_gc_profiling_bahan_kirim.csv'

# kolom yg harus ada di HASIL_GC
required_cols = {
    "idsbr",
    "Petugas GC",
    "nama usaha hasil update",
    "hasil update keberadaan usaha",
    "latitude_update",
    "longitude_update",
    "apakah sudah diinput di matchapro mobile?"
}

schema = {
    'hasil update keberadaan usaha': 'Int64'
}

def fix_longitude(x):
    if pd.isna(x) or str(x).strip() == '':
        return np.nan

    s = str(x).strip()
    s = s.replace('..', '.').replace(',', '.')

    try:
        x = float(s)
    except ValueError:
        return np.nan

    candidates = [x]

    for _ in range(10):
        x /= 10
        candidates.append(x)

    valid = [c for c in candidates if 95 <= c <= 141]

    if not valid:
        return np.nan

    return min(valid, key=lambda v: abs(v - 118))

def fix_latitude(x):
    if pd.isna(x) or str(x).strip() == '':
        return np.nan

    s = str(x).strip()
    s = s.replace('..', '.').replace(',', '.')

    try:
        x = float(s)
    except ValueError:
        return np.nan

    candidates = [x]

    for _ in range(10):
        x /= 10
        candidates.append(x)

    valid = [c for c in candidates if -11 <= c <= 6]

    if not valid:
        return np.nan

    return min(valid, key=lambda v: abs(v + 3))

def main():
    dfs = {}

    for SHEET in SHEETS:
        try:
            dfs[SHEET] = pd.read_excel(HASIL_GC, sheet_name=SHEET)
            
            missing = required_cols - set(dfs[SHEET].columns)
            if missing:
                raise KeyError(f"Missing columns: {', '.join(missing)}")
            
            print(f"Berhasil membaca sheet {SHEET}")
        except KeyError as e:
            print(f"Gagal membaca sheet {SHEET} - Detail error: {e}")
            sys.exit(1)

    # filter df hasil gc u
    frames = []

    for df in dfs.values():
        mask = (
            (df['Petugas GC'] == PETUGAS_GC) &
            (df['apakah sudah diinput di matchapro mobile?'] == False) &
            (df['hasil update keberadaan usaha'].notna())
        )

        if mask.any():
            frames.append(df[mask])

    filtered = pd.concat(frames, ignore_index=True)

    # import file hasil scrapping
    df = pd.read_csv('direktori_usaha_full_all_columns_2026.csv', low_memory=False)
    nama_usaha_gc_untouched = df['nama_usaha_gc']

    # merge hasil scrapping sama hasil gc
    cols_to_merge = ['idsbr', "latitude_update", "longitude_update", "nama usaha hasil update", "hasil update keberadaan usaha"]

    if DROP_ALL_USERNAME:
        base_df = df[df['gc_username'].isna()]
    else:
        base_df = df.loc[
            df['gc_username'].isin([USERNAME]) | df['gc_username'].isna()
        ]
    
    result = (
        base_df
        .merge(
            filtered[cols_to_merge],
            on='idsbr',
            how='inner'
        )
    )

    result['nama_usaha_gc'] = nama_usaha_gc_untouched.loc[result.index].values

    print(f"Total hasil gc: {len(result)} record")

    # update longlat ke hasil scrapping
    result['latitude'] = (
        result['latitude_update']
        .combine_first(result['latitude_gc'])
        .combine_first(result['latitude'])
    )

    result['longitude'] = (
        result['longitude_update']
        .combine_first(result['longitude_gc'])
        .combine_first(result['longitude'])
    )

    # benerin format longlat
    result['latitude'] = result['latitude'].apply(fix_latitude)
    result['longitude'] = result['longitude'].apply(fix_longitude)

    result = result.astype(schema)

    # benerin struktur kolom
    result = result.rename(columns={
        'nama usaha hasil update': 'nama_usaha_edit',
        'hasil update keberadaan usaha': 'hasilgc'
    })

    result['alamat_usaha_edit'] = ''

    # simpan hasil gc
    result.to_csv(BAHAN_KIRIM)
    print(f"Output disimpan ke: {BAHAN_KIRIM}")

if __name__ == "__main__":
    main()