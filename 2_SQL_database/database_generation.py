"""
database_generation.py
Sets up the thesis DuckDB database from cleaned MCAS xlsx files and Banxico remittances.

Run from anywhere:
    python Thesis/2_SQL_database/database_generation.py

Prerequisites:
    Run USESTADO_4_clean_remittances.R first to generate the remittance parquets.
"""

import re
import unicodedata
import duckdb
import pandas as pd
from pathlib import Path


def normalize_name(s: str) -> str:
    """
    Matches normalise_name() in USESTADO_4_clean_remittances.R:
    iconv ASCII//TRANSLIT → lowercase → strip non-[a-z0-9 ] → collapse spaces.
    Used to join dim table names (Title Case + accents) against parquet _norm columns.
    """
    nfkd = unicodedata.normalize("NFKD", str(s))
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^a-z0-9 ]", "", ascii_str.lower())
    return re.sub(r" +", " ", cleaned).strip()

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

THESIS_DIR   = Path(__file__).resolve().parent.parent   # .../Thesis/
DB_PATH      = THESIS_DIR / "2_SQL_database/thesis.duckdb"
ESTADOS_DIR  = THESIS_DIR / "2_SQL_database/Data_clean_updated/MCAS/Estados_US"
REMIT_DIR    = THESIS_DIR / "2_SQL_database/Data_clean_updated/Remittances"
YEARS        = range(2010, 2025)

# ─────────────────────────────────────────────────────────────
# CONNECT & CREATE TABLES
# ─────────────────────────────────────────────────────────────

con = duckdb.connect(str(DB_PATH))

con.execute("""
    CREATE TABLE IF NOT EXISTS dim_mx_state (
        mx_state_id INTEGER PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE
    )
""")
con.execute("""
    CREATE TABLE IF NOT EXISTS dim_mx_municipality (
        mx_muni_id  INTEGER PRIMARY KEY,
        name        TEXT NOT NULL,
        mx_state_id INTEGER NOT NULL,
        UNIQUE (name, mx_state_id)
    )
""")
con.execute("""
    CREATE TABLE IF NOT EXISTS dim_us_state (
        us_state_id  INTEGER PRIMARY KEY,
        name         TEXT NOT NULL UNIQUE,
        abbreviation CHAR(2)
    )
""")
con.execute("""
    CREATE TABLE IF NOT EXISTS fact_mcas (
        mx_muni_id     INTEGER  NOT NULL,
        us_state_id    INTEGER  NOT NULL,
        year           SMALLINT NOT NULL,
        n_matriculas   INTEGER,
        pct_matriculas REAL,
        PRIMARY KEY (mx_muni_id, us_state_id, year)
    )
""")
con.execute("""
    CREATE TABLE IF NOT EXISTS fact_remittance_us (
        us_state_id  INTEGER  NOT NULL,
        year         SMALLINT NOT NULL,
        quarter      TINYINT  NOT NULL,
        remit_musd   DOUBLE,
        PRIMARY KEY (us_state_id, year, quarter)
    )
""")
con.execute("""
    CREATE TABLE IF NOT EXISTS fact_remittance_mx_muni (
        mx_muni_id   INTEGER  NOT NULL,
        year         SMALLINT NOT NULL,
        quarter      TINYINT  NOT NULL,
        remit_musd   DOUBLE,
        PRIMARY KEY (mx_muni_id, year, quarter)
    )
""")
con.execute("""
    CREATE TABLE IF NOT EXISTS fact_remittance_mx_state (
        mx_state_id  INTEGER  NOT NULL,
        year         SMALLINT NOT NULL,
        quarter      TINYINT  NOT NULL,
        remit_musd   DOUBLE,
        PRIMARY KEY (mx_state_id, year, quarter)
    )
""")

# ─────────────────────────────────────────────────────────────
# STEP 1: Read all xlsx files into one combined DataFrame
# ─────────────────────────────────────────────────────────────

print("Reading all xlsx files...")
chunks = []

for year in YEARS:
    folder = ESTADOS_DIR / f"Edos_USA_{year}"
    if not folder.exists():
        print(f"  [SKIP] {folder.name} not found")
        continue

    xlsx_files = sorted(folder.glob("*_*.xlsx"))
    print(f"  {year}: {len(xlsx_files)} files")

    for path in xlsx_files:
        if path.name.startswith("._"):
            continue
        df = pd.read_excel(path)
        if not {"mx_state", "mx_municipality", "n_matriculas", "pct_matriculas"}.issubset(df.columns):
            print(f"    [SKIP - wrong columns] {path.name}: {list(df.columns)}")
            continue
        df = df[["mx_state", "mx_municipality", "n_matriculas", "pct_matriculas"]].copy()
        df["year"]         = year
        df["us_state_name"] = path.stem.replace(f"_{year}", "").replace("_", " ").title()
        chunks.append(df)

print(f"\nCombining {len(chunks)} files...")
combined = pd.concat(chunks, ignore_index=True)
combined = combined.dropna(subset=["mx_state", "mx_municipality"])
print(f"Total rows: {len(combined):,}")

# ─────────────────────────────────────────────────────────────
# STEP 2: Build dimension tables from unique values
# ─────────────────────────────────────────────────────────────

print("\nBuilding dimension tables...")

# dim_us_state
us_states = pd.DataFrame({
    "us_state_id": range(1, combined["us_state_name"].nunique() + 1),
    "name":        sorted(combined["us_state_name"].unique())
})
con.execute("INSERT OR IGNORE INTO dim_us_state (us_state_id, name) SELECT us_state_id, name FROM us_states")
print(f"  dim_us_state: {len(us_states)} rows")

# dim_mx_state
mx_states = pd.DataFrame({
    "mx_state_id": range(1, combined["mx_state"].nunique() + 1),
    "name":        sorted(combined["mx_state"].unique())
})
con.execute("INSERT OR IGNORE INTO dim_mx_state SELECT mx_state_id, name FROM mx_states")
print(f"  dim_mx_state: {len(mx_states)} rows")

# dim_mx_municipality — unique (name, mx_state) pairs
mx_munis_raw = (
    combined[["mx_state", "mx_municipality"]]
    .drop_duplicates()
    .sort_values(["mx_state", "mx_municipality"])
    .reset_index(drop=True)
)
mx_munis_raw["mx_muni_id"] = range(1, len(mx_munis_raw) + 1)
mx_munis_raw = mx_munis_raw.merge(mx_states.rename(columns={"name": "mx_state"}), on="mx_state")
con.execute("""
    INSERT OR IGNORE INTO dim_mx_municipality
    SELECT mx_muni_id, mx_municipality AS name, mx_state_id
    FROM mx_munis_raw
""")
print(f"  dim_mx_municipality: {len(mx_munis_raw)} rows")

# ─────────────────────────────────────────────────────────────
# STEP 3: Bulk insert fact_mcas
# ─────────────────────────────────────────────────────────────

print("\nInserting fact_mcas...")

con.execute("""
    INSERT OR IGNORE INTO fact_mcas
    SELECT
        m.mx_muni_id,
        u.us_state_id,
        c.year,
        c.n_matriculas,
        c.pct_matriculas
    FROM combined c
    JOIN mx_states  mx ON mx.name        = c.mx_state
    JOIN mx_munis_raw m ON m.mx_state_id = mx.mx_state_id
                       AND m.mx_municipality = c.mx_municipality
    JOIN us_states  u  ON u.name         = c.us_state_name
""")

count = con.execute("SELECT COUNT(*) FROM fact_mcas").fetchone()[0]
print(f"  fact_mcas: {count:,} rows inserted")

# ─────────────────────────────────────────────────────────────
# STEP 4: Remittance tables (from USESTADO_4 parquets)
# ─────────────────────────────────────────────────────────────

print("\nInserting remittance data...")

remit_us_path    = REMIT_DIR / "remit_us_states.parquet"
remit_muni_path  = REMIT_DIR / "remit_mx_munis.parquet"
remit_state_path = REMIT_DIR / "remit_mx_states.parquet"

missing = [p for p in [remit_us_path, remit_muni_path, remit_state_path] if not p.exists()]
if missing:
    print(f"  [SKIP] Remittance parquets not found. Run USESTADO_4_clean_remittances.R first.")
    print(f"  Missing: {[p.name for p in missing]}")
else:
    # ── fact_remittance_us ────────────────────────────────────
    remit_us = pd.read_parquet(remit_us_path)
    # Exclude Total and No identificado rows
    remit_us = remit_us[~remit_us["us_state"].isin(["Total", "No identificado"])]

    # Join to dim_us_state — normalize both sides to handle any casing differences
    dim_us = con.execute("SELECT us_state_id, name FROM dim_us_state").df()
    dim_us["name_norm"] = dim_us["name"].apply(normalize_name)
    remit_us["us_state_norm"] = remit_us["us_state"].apply(normalize_name)
    remit_us_joined = remit_us.merge(dim_us, left_on="us_state_norm", right_on="name_norm", how="left")

    unmatched_us = remit_us_joined[remit_us_joined["us_state_id"].isna()]["us_state"].unique()
    if len(unmatched_us):
        print(f"  [WARN] DS2 states not in dim_us_state: {unmatched_us.tolist()}")

    remit_us_joined = remit_us_joined.dropna(subset=["us_state_id"])
    remit_us_joined["us_state_id"] = remit_us_joined["us_state_id"].astype(int)

    con.execute("""
        INSERT OR IGNORE INTO fact_remittance_us
        SELECT us_state_id, year, quarter, remit_musd
        FROM remit_us_joined
    """)
    count_us = con.execute("SELECT COUNT(*) FROM fact_remittance_us").fetchone()[0]
    print(f"  fact_remittance_us: {count_us:,} rows")

    # ── fact_remittance_mx_state ──────────────────────────────
    remit_mxst = pd.read_parquet(remit_state_path)
    dim_mxst = con.execute("SELECT mx_state_id, name FROM dim_mx_state").df()
    dim_mxst["name_norm"] = dim_mxst["name"].apply(normalize_name)
    remit_mxst_joined = remit_mxst.merge(
        dim_mxst, left_on="mx_state_norm", right_on="name_norm", how="left"
    )

    unmatched_mxst = remit_mxst_joined[remit_mxst_joined["mx_state_id"].isna()]["mx_state"].unique()
    if len(unmatched_mxst):
        print(f"  [WARN] DS1 MX states not in dim_mx_state: {unmatched_mxst.tolist()}")

    remit_mxst_joined = remit_mxst_joined.dropna(subset=["mx_state_id"])
    remit_mxst_joined["mx_state_id"] = remit_mxst_joined["mx_state_id"].astype(int)

    con.execute("""
        INSERT OR IGNORE INTO fact_remittance_mx_state
        SELECT mx_state_id, year, quarter, remit_musd
        FROM remit_mxst_joined
    """)
    count_mxst = con.execute("SELECT COUNT(*) FROM fact_remittance_mx_state").fetchone()[0]
    print(f"  fact_remittance_mx_state: {count_mxst:,} rows")

    # ── fact_remittance_mx_muni ───────────────────────────────
    remit_mx = pd.read_parquet(remit_muni_path)
    dim_muni = con.execute("""
        SELECT m.mx_muni_id, m.name AS muni_name, s.name AS state_name
        FROM dim_mx_municipality m JOIN dim_mx_state s USING (mx_state_id)
    """).df()
    dim_muni["state_norm"] = dim_muni["state_name"].apply(normalize_name)
    dim_muni["muni_norm"]  = dim_muni["muni_name"].apply(normalize_name)

    remit_mx_joined = remit_mx.merge(
        dim_muni,
        left_on=["mx_state_norm", "mx_muni_norm"],
        right_on=["state_norm", "muni_norm"],
        how="left"
    )

    n_total      = len(remit_mx_joined["mx_muni_norm"].unique())
    n_unmatched  = remit_mx_joined[remit_mx_joined["mx_muni_id"].isna()]["mx_muni_norm"].nunique()
    print(f"  DS1 municipality match rate: {n_total - n_unmatched}/{n_total} "
          f"({100*(n_total-n_unmatched)/n_total:.1f}%)")
    if n_unmatched > 0:
        sample = (remit_mx_joined[remit_mx_joined["mx_muni_id"].isna()]
                  .groupby(["mx_state", "mx_muni"])["remit_musd"].sum()
                  .nlargest(10))
        print(f"  Largest unmatched (by total remittances):\n{sample.to_string()}")

    remit_mx_joined = remit_mx_joined.dropna(subset=["mx_muni_id"])
    remit_mx_joined["mx_muni_id"] = remit_mx_joined["mx_muni_id"].astype(int)

    # Aggregate in case a muni matched multiple dim rows (shouldn't happen, safety net)
    remit_mx_agg = (
        remit_mx_joined
        .groupby(["mx_muni_id", "year", "quarter"], as_index=False)["remit_musd"]
        .sum()
    )

    con.execute("""
        INSERT OR IGNORE INTO fact_remittance_mx_muni
        SELECT mx_muni_id, year, quarter, remit_musd
        FROM remit_mx_agg
    """)
    count_mx = con.execute("SELECT COUNT(*) FROM fact_remittance_mx_muni").fetchone()[0]
    print(f"  fact_remittance_mx_muni: {count_mx:,} rows")

con.close()
print("\nDone.")

