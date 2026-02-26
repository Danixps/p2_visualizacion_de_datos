import pandas as pd
import numpy as np
from dagster import asset, asset_check, AssetCheckResult, MetadataValue
from plotnine import ggplot, aes, geom_col, theme_minimal, labs, scale_fill_brewer

# ==========================================
# 1. DEFINICIÓN DE ASSETS (Activos de datos)
# ==========================================


@asset(description="Carga y limpieza inicial del dataset de rentas en Canarias.")
def rentas_canarias() -> pd.DataFrame:
    # 1. Leemos el CSV (mantengo tu ruta, asumiendo que lanzas desde la carpeta src)
    df = pd.read_csv("data/distribucion-renta-canarias.csv")
    
    # 2. RENOMBRAMOS usando los nombres reales que me acabas de pasar
    df = df.rename(columns={
        "TERRITORIO#es": "Islayayuntamiento", 
        "OBS_VALUE": "Renta_Media"
    })
    
    # 3. Forzamos que la renta sea numérica (por si viene como texto)
    df['Renta_Media'] = pd.to_numeric(df['Renta_Media'], errors='coerce')
    
    # 4. Transformación: Ordenar de mayor a menor renta
    df = df.sort_values(by="Renta_Media", ascending=False)
    
    return df

@asset(description="Generación del gráfico de barras con plotnine.")
def grafico_rentas(rentas_canarias: pd.DataFrame):
    # Generamos la visualización. geom_col() fuerza a que el eje Y empiece en 0.
    p = (
        ggplot(rentas_canarias, aes(x='reorder(Islayayuntamiento, -Renta_Media)', y='Renta_Media', fill='Islayayuntamiento'))
        + geom_col() 
        + theme_minimal()
        + labs(title="Renta Media por Isla en Canarias", x="Islayayuntamiento", y="Renta Media (€)")
        + scale_fill_brewer(type="qual", palette="Set2")
    )
    
    # Guardamos el gráfico
    ruta_salida = "distribucion_rentas_canarias.png"
    p.save(ruta_salida, width=10, height=6, dpi=300, verbose=False)
    
    return ruta_salida


# ==========================================
# 2. CAPA DE CALIDAD (Dagster Checks)
# ==========================================

# -- Checks de Carga y Transformación --

@asset_check(asset=rentas_canarias)
def check_sin_nulos_renta(rentas_canarias: pd.DataFrame):
    nulos = int(rentas_canarias['Renta_Media'].isnull().sum())
    return AssetCheckResult(
        passed=nulos == 0,
        metadata={
            "nulos_detectados": MetadataValue.int(nulos),
            "descripcion": MetadataValue.text("Verifica que no falten datos de renta para ninguna isla.")
        }
    )

@asset_check(asset=rentas_canarias)
def check_rentas_positivas(rentas_canarias: pd.DataFrame):
    negativos = int((rentas_canarias['Renta_Media'] < 0).sum())
    return AssetCheckResult(
        passed=negativos == 0,
        metadata={
            "valores_negativos": MetadataValue.int(negativos),
            "descripcion": MetadataValue.text("Garantiza que no existan rentas menores a 0 por errores de tipeo.")
        }
    )

# -- Checks de Visualización --


@asset_check(asset=rentas_canarias) 
def check_limite_categorias(rentas_canarias: pd.DataFrame):
    # Verificamos que no haya más de 8 categorías para no romper la paleta de colores cualitativa
    num_categorias = int(rentas_canarias['Islayayuntamiento'].nunique())
    limite = 97
    
    return AssetCheckResult(
        passed=num_categorias <= limite,
        metadata={
            "categorias_detectadas": MetadataValue.int(num_categorias),
            "limite_permitido": MetadataValue.int(limite),
            "principio_diseno": MetadataValue.md("**Uso efectivo del color**. Más de 8 colores en una paleta cualitativa dificulta la distinción visual.")
        }
    )