import pandas as pd
from plotnine import *
from dagster import asset, AssetCheckResult, asset_check

# --- 1. ASSETS DE EXTRACCIÓN ---

@asset
def renta_raw() -> pd.DataFrame:
    """Carga los datos brutos de distribución de renta."""
    return pd.read_csv('data/distribucion-renta-canarias.csv')

@asset
def estudios_raw() -> pd.DataFrame:
    """Carga el Excel de niveles de estudios."""
    return pd.read_excel('data/nivelestudios.xlsx')

@asset
def codislas_raw() -> pd.DataFrame:
    """Carga el diccionario de islas con el separador y encoding correctos."""
    return pd.read_csv('data/codislas.csv', sep=';', encoding='latin1', dtype=str)

# --- 2. ASSETS DE TRANSFORMACIÓN ---

@asset(deps=[renta_raw, estudios_raw, codislas_raw])
def dataset_municipios_completo(renta_raw: pd.DataFrame, estudios_raw: pd.DataFrame, codislas_raw: pd.DataFrame) -> pd.DataFrame:
    # 1. Limpieza Renta (2023)
    df_renta = renta_raw[(renta_raw['TIME_PERIOD_CODE'] == 2023) & 
                        (renta_raw['MEDIDAS_CODE'] == 'SUELDOS_SALARIOS')].copy()
    df_renta['cod_municipio'] = df_renta['TERRITORIO_CODE'].astype(str).str.strip()

    # 2. Limpieza Estudios (Suma de categorías para evitar el 0)
    col_mun = 'Municipios de 500 habitantes o más'
    estudios_raw['cod_municipio'] = estudios_raw[col_mun].str.extract(r'(\d{5})')
    # Agrupamos por código de municipio y sumamos la columna 'Total'
    df_estudios_sum = estudios_raw.groupby('cod_municipio')['Total'].sum().reset_index()

    # 3. Limpieza Islas (CORREGIDO EL ERROR 'cod')
    # Unimos CPRO (2 dígitos) + CMUN (3 dígitos) para crear el código de 5 dígitos
    codislas_raw['cod_municipio'] = codislas_raw['CPRO'].str.zfill(2) + codislas_raw['CMUN'].str.zfill(3)
    df_islas = codislas_raw.rename(columns={'ISLA': 'Isla'})
    
    # 4. Cruce Final (Merge)
    # Primero unimos Renta con Islas
    df_final = pd.merge(df_renta, df_islas[['cod_municipio', 'Isla']], on='cod_municipio', how='inner')
    # Luego unimos con la suma de estudios
    df_final = pd.merge(df_final, df_estudios_sum, on='cod_municipio', how='inner')
    
    # Formateo numérico final
    df_final['Renta'] = pd.to_numeric(df_final['OBS_VALUE'], errors='coerce')
    df_final['Poblacion_Estudios'] = pd.to_numeric(df_final['Total'], errors='coerce')
    
    return df_final.dropna(subset=['Renta', 'Poblacion_Estudios'])

# --- 3. VALIDACIONES Y VISUALIZACIÓN ---

@asset_check(asset=dataset_municipios_completo)
def check_municipios_canarias(dataset_municipios_completo: pd.DataFrame):
    num_municipios = len(dataset_municipios_completo)
    return AssetCheckResult(
        passed=bool(num_municipios > 0), 
        metadata={"municipios_detectados": num_municipios}
    )

@asset(deps=[dataset_municipios_completo])
def grafico_renta_estudios(dataset_municipios_completo: pd.DataFrame):
    """Genera la visualización final."""
    grafico = (
        ggplot(dataset_municipios_completo, aes(x='Poblacion_Estudios', y='Renta', color='Isla'))
        + geom_point(size=3, alpha=0.8)
        + theme_minimal()
        + labs(
            title="Relación entre Nivel de Estudios y Renta en Canarias",
            x="Población Total (Nivel Estudios)",
            y="Sueldos y Salarios (€)",
            color="Isla"
        )
    )
    
    ruta_salida = 'output/grafico_renta_estudios_dagster.png'
    grafico.save(ruta_salida, width=10, height=6, dpi=300)
    
    return f"Gráfico guardado exitosamente en {ruta_salida}"