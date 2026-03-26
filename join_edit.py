import pandas as pd

def main():
    matcha = pd.read_csv("direktori_usaha_full_all_columns_2026.csv")
    final = pd.read_excel("df_final.xlsx")

    result = (
            matcha
            .merge(
                final,
                on='idsbr',
                how='inner'
            )
        )

    result = result.rename(columns={
        "hasil update keberadaan usaha\n\n1. Ditemukan\n3. Tutup\n4. Ganda\n99.  Tidak Ditemukan": "hasilgc"
    })

    result.to_csv('data_gc_profiling_bahan_kirim.csv')

if __name__ == "__main__":
    main()