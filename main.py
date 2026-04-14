import yaml
from pathlib import Path
from pipeline import (
    load_data,
    normalize_data,
    apply_rules,
    detect_anomalies,
    build_outputs,
    data_quality_summary
)

def load_config(path="config.yaml"):
    with open(path, "r") as file:
        return yaml.safe_load(file)

# Configuración de rutas base
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

def run_pipeline():
    # 1. CARGA REAL DE CONFIGURACIÓN
    config_path = BASE_DIR / "config.yaml"
    
    if not config_path.exists():
        print(f"❌ ERROR: No se encontró el archivo en: {config_path}")
        return

    config = load_config(config_path)
    multiplier = config.get("outliers", {}).get("multiplier", 1.5)

    # 2. Definición de rutas de archivos
    data_path = PROJECT_ROOT / config["paths"]["input_data"]
    output_reporting = PROJECT_ROOT / config["paths"]["output_reporting"]
    output_anomalies = PROJECT_ROOT / config["paths"]["output_anomalies"]
    output_summary = PROJECT_ROOT / config["paths"]["output_summary"]

    # 3. Crear carpeta de salida si no existe
    output_reporting.parent.mkdir(parents=True, exist_ok=True)

    try:
        print(f"🚀 Iniciando Pipeline... leyendo {data_path.name}")
        
        # Ejecución secuencial
        df = load_data(data_path)
        df = normalize_data(df)
        df = apply_rules(df)
        df = detect_anomalies(df, multiplier)

        reporting_df, anomalies_df = build_outputs(df)
        summary_df = data_quality_summary(df)

        # Guardar resultados
        reporting_df.to_csv(output_reporting, index=False)
        anomalies_df.to_csv(output_anomalies, index=False)
        summary_df.to_csv(output_summary, index=False)

        # 4. IMPRESIONES (Dentro del try para asegurar que existan)
        print("\n=== DATA QUALITY SUMMARY ===")
        print(summary_df)

        print("\n=== REPORTING SAMPLE (Top 5) ===")
        print(reporting_df.head())

        print("\n=== ANOMALIES SAMPLE (Top 5) ===")
        print(anomalies_df.head())

        print(f"\n✅ Pipeline finalizado. Resultados en: {output_reporting.parent}")

    except ValueError as e:
        print(f"\n❌ Error de validación: {e}")
    except FileNotFoundError:
        print(f"\n❌ Error: El archivo de datos no existe en {data_path}")
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")

if __name__ == "__main__":
    # Mensaje inicial decorativo
    print("\n" + "="*30)
    print("  DATA QUALITY PIPELINE")
    print("="*30)
    
    run_pipeline()