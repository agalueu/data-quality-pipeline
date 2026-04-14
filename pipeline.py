import pandas as pd

required_columns = [
        'transaction_id', 'user_id', 'transaction_date', 'amount', 
        'currency', 'status', 'payment_method', 'country', 
        'created_at', 'source_system'
    ]

def load_data(path):
    
    try:
        df = pd.read_csv(path)
    except Exception as e:
        raise ValueError(f"Error leyendo el archivo: {e}")
   
    if df.empty:
        raise ValueError ("El dataset está vacío")

    missing_columns = set(required_columns) - set(df.columns)
        
    if missing_columns:
        raise ValueError (f"Faltan columnas en el dataset: {missing_columns}")
            
    return df # Si todo está bien, entrega el dataframe
        

def normalize_data(df): #TIPADO Y FECHAS
    df = df.copy()

    df["amount_numeric"] = pd.to_numeric(df["amount"], errors="coerce")
    df["transaction_date_parsed"] = pd.to_datetime(df["transaction_date"], errors="coerce")
    df["created_at_parsed"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["status_normalized"] = df["status"].str.strip().str.lower()

    return df


def apply_rules(df): #APLICAR REGLAS
    df = df.copy()

    # Flags de error
    df["invalid_tx_id"] = df["transaction_id"].isna()
    df["invalid_user"] = df["user_id"].isna()
    df["invalid_tx_date"] = df["transaction_date_parsed"].isna()

    df["invalid_amount"] = (
        df["amount_numeric"].isna() |
        (df["amount_numeric"] == 0) |
        ((df["amount_numeric"] < 0) & (df["status_normalized"] == "approved"))
    )

    df["invalid_created"] = df["created_at_parsed"].isna()
    df["is_duplicate"] = df.duplicated(subset=["transaction_id"], keep=False)

    # Reglas
    df["rule_user_valid"] = ~df["invalid_user"]
    df["rule_amount_valid"] = ~df["invalid_amount"]
    df["rule_tx_date_valid"] = ~df["invalid_tx_date"]
    df["rule_created_valid"] = ~df["invalid_created"]
    df["rule_not_duplicate"] = ~df["is_duplicate"]
    df["rule_tx_id_valid"] = ~df["invalid_tx_id"]

    # Resultado final
    df["is_valid"] = (
        df["rule_user_valid"] &
        df["rule_amount_valid"] &
        df["rule_tx_date_valid"] &
        df["rule_created_valid"] &
        df["rule_not_duplicate"] &
        df["rule_tx_id_valid"]
    )

    return df


def detect_anomalies(df, multiplier=1.5):
    df = df.copy()

    valid_df = df[df["is_valid"]]

    if len(valid_df) == 0:
        df["flag_amount_outlier"] = False
        return df

    Q1 = valid_df["amount_numeric"].quantile(0.25)
    Q3 = valid_df["amount_numeric"].quantile(0.75)
    IQR = Q3 - Q1

    upper_bound = Q3 + multiplier * IQR

    df["flag_amount_outlier"] = df["amount_numeric"] > upper_bound

    return df


def build_outputs(df):
    reporting_df = df[
        (df["is_valid"]) &
        (df["status_normalized"] == "approved")
    ].copy()

    anomalies_df = df[
        (~df["is_valid"]) |
        (df["flag_amount_outlier"])
    ].copy()

    return reporting_df, anomalies_df

def data_quality_summary(df):
    return pd.DataFrame({
        "metric": [
            "total_rows",
            "valid_rows",
            "invalid_rows",
            "duplicates",
            "amount_outliers"
        ],
        "value": [
            len(df),
            df["is_valid"].sum(),
            (~df["is_valid"]).sum(),
            df["is_duplicate"].sum(),
            df["flag_amount_outlier"].sum()
        ]
    })