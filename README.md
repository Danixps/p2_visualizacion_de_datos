# 📊 Análisis Socioeconómico: Renta Media vs. Nivel de Estudios en Canarias

Este proyecto implementa un pipeline de datos profesional para analizar la correlación entre el nivel educativo y la renta media en los municipios de las Islas Canarias. La solución utiliza **Dagster** para la orquestación, **Pandas** para la ingeniería de datos, **Plotnine (ggplot2)** para la visualización y **GitHub Actions** para la integración continua (CI).



## 🎯 Objetivo del Proyecto

El propósito es investigar si existe una relación estadística entre el volumen de población con estudios y la renta media (sueldos y salarios) por municipio. El proyecto resuelve la fragmentación de datos provenientes de distintas fuentes oficiales (ISTAC, INE) y unifica la información mediante un identificador geográfico común.

## 🛠️ Tecnologías Utilizadas

* **Lenguaje:** Python 3.12+
* **Orquestador:** [Dagster](https://dagster.io/) (Software-Defined Assets)
* **Procesamiento:** [Pandas](https://pandas.pydata.org/)
* **Visualización:** [Plotnine](https://plotnine.readthedocs.io/) (Implementación de *The Grammar of Graphics*)
* **CI/CD:** GitHub Actions
* **Formatos de entrada:** CSV (con diferentes delimitadores y encodings) y Excel (.xlsx)

## 🏗️ Arquitectura del Pipeline (ETL)

### 1. Extracción (Extract)
Se gestionan tres activos de datos brutos:
* `renta_raw`: Datos de distribución de renta (CSV).
* `estudios_raw`: Niveles de estudio por municipio (Excel).
* `codislas_raw`: Diccionario de códigos municipales e islas (CSV con delimitador `;` y codificación `latin1`).

### 2. Transformación (Transform)
El activo central `dataset_municipios_completo` realiza las siguientes operaciones críticas:
* **Normalización Geográfica:** Se genera un código INE de 5 dígitos concatenando `CPRO` (Provincia) y `CMUN` (Municipio), aplicando `zfill` para asegurar ceros a la izquierda.
* **Limpieza de Strings:** Uso de expresiones regulares (`regex`) para extraer códigos numéricos de cadenas de texto complejas en el Excel de estudios.
* **Agregación de Datos:** Dado que los datos de estudios están desagregados por sexo y nacionalidad, se aplica una agregación `groupby().sum()` para obtener el total municipal.
* **Alineación Temporal:** Filtrado de datos de renta del periodo 2023 para garantizar la relevancia actual.



### 3. Carga y Visualización (Load)
El activo `grafico_renta_estudios` genera un gráfico de dispersión (Scatter Plot) configurado con:
* **Eje X:** Población Total con Estudios.
* **Eje Y:** Renta Media (Sueldos y Salarios en €).
* **Color:** Segmentación por Isla.

## 🔄 Integración Continua (CI)

Se ha implementado un flujo de trabajo en **GitHub Actions** (`.github/workflows/ci.yml`) que se dispara automáticamente en cada `push`. Este flujo realiza:
1.  Instalación del entorno y dependencias.
2.  **Linting/Syntax Check:** Verifica que no existan errores de nombres no definidos o errores de sintaxis en el código.
3.  **Data Integrity Check:** Asegura que los archivos de datos necesarios estén presentes en el repositorio antes de la ejecución.



## 📂 Estructura del Repositorio

```text
.
├── .github/workflows/
│   └── ci.yml                # Configuración de Integración Continua
├── data/                     # Archivos fuente (Renta, Estudios, Islas)
│   ├── codislas.csv
│   ├── distribucion-renta-canarias.csv
│   └── nivelestudios.xlsx
├── src/                      # Código fuente
│   ├── pipeline.py           # Pipeline principal de Dagster
│   └── lab-renta.py          # Script de prototipado y pruebas
├── output/                   # Resultados de las visualizaciones (.png)
└── README.md
