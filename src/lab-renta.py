import pandas as pd
from plotnine import *
import warnings

warnings.filterwarnings('ignore')

def main():
    print("1. Cargando datasets...")
    try:
        df_renta = pd.read_csv('data/distribucion-renta-canarias.csv')
        df_estudios = pd.read_excel('data/nivelestudios.xlsx')
        df_islas = pd.read_csv('data/codislas.csv', sep=';', encoding='latin1', dtype=str)
        print("✅ Archivos cargados.")
    except Exception as e:
        print(f"❌ Error al cargar archivos: {e}")
        return

    print("2. Limpiando datos...")
    
    # --- RENTA ---
    # Filtramos por 2023 y Sueldos (que es el dato más reciente que tienes en renta)
    df_renta = df_renta[(df_renta['TIME_PERIOD_CODE'] == 2023) & 
                        (df_renta['MEDIDAS_CODE'] == 'SUELDOS_SALARIOS')].copy()
    df_renta['cod_municipio'] = df_renta['TERRITORIO_CODE'].astype(str).str.strip()

    # --- ESTUDIOS ---
    col_mun = 'Municipios de 500 habitantes o más'
    # Extraer el código numérico de 5 dígitos
    df_estudios['cod_municipio'] = df_estudios[col_mun].str.extract(r'(\d{5})')
    
    # IMPORTANTE: Como en tus datos hay filas por Sexo y Nacionalidad, 
    # sumamos todos los valores para tener el TOTAL por municipio
    df_estudios_clean = df_estudios.groupby('cod_municipio')['Total'].sum().reset_index()
    df_estudios_clean = df_estudios_clean.rename(columns={'Total': 'Poblacion_Estudios'})

    # --- ISLAS ---
    # Unir Provincia + Municipio
    df_islas['cod_municipio'] = df_islas['CPRO'].str.zfill(2) + df_islas['CMUN'].str.zfill(3)
    df_islas = df_islas.rename(columns={'ISLA': 'Isla'})

    print("3. Uniendo datos...")
    # Cruzamos todo por 'cod_municipio'
    df_final = pd.merge(df_renta, df_islas[['cod_municipio', 'Isla']], on='cod_municipio', how='inner')
    df_final = pd.merge(df_final, df_estudios_clean, on='cod_municipio', how='inner')

    # Convertir a números
    df_final['Renta'] = pd.to_numeric(df_final['OBS_VALUE'], errors='coerce')
    df_final['Poblacion_Estudios'] = pd.to_numeric(df_final['Poblacion_Estudios'], errors='coerce')
    df_final = df_final.dropna(subset=['Renta', 'Poblacion_Estudios'])

    print(f"📊 ¡Ahora sí! Procesados {len(df_final)} municipios.")

    if len(df_final) == 0:
        print("⚠️ El resultado sigue siendo 0. Mostrando códigos para debug:")
        print(f"Códigos en Renta: {df_renta['cod_municipio'].unique()[:5]}")
        print(f"Códigos en Estudios: {df_estudios_clean['cod_municipio'].unique()[:5]}")
        return

    print("4. Generando gráfico...")
    grafico = (
        ggplot(df_final, aes(x='Poblacion_Estudios', y='Renta', color='Isla'))
        + geom_point(size=3, alpha=0.7)
        + theme_minimal()
        + labs(
            title="Relación Estudios vs Renta en Canarias",
            subtitle="Nota: Datos de estudios (2021) y renta (2023)",
            x="Población Total con Estudios (Suma de categorías)",
            y="Renta Media (Sueldos y Salarios)",
            color="Isla"
        )
    )
    
    grafico.save('output/grafico_renta_estudios.png', width=10, height=6, dpi=300)
    print("✅ Gráfico guardado en 'output/grafico_renta_estudios.png'")

if __name__ == "__main__":
    main()