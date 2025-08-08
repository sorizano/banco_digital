import os
import sys
import time
import json
import argparse
import random
from datetime import datetime
import pandas as pd

DATA_DIR = "data"
STREAM_DIR = "stream"
CLEAN_PATH = os.path.join(DATA_DIR, "clean.pkl")
EVENTS_PATH = os.path.join(STREAM_DIR, "events.jsonl")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(STREAM_DIR, exist_ok=True)

ACTIONS = ["view", "click", "purchase", "refund"]


def load_clean_df():
    if not os.path.exists(CLEAN_PATH):
        raise FileNotFoundError(f"No se encontró {CLEAN_PATH}. Ejecuta el ETL primero: python etl_clean_load.py")
    return pd.read_pickle(CLEAN_PATH)


def produce(n: int):
    df = load_clean_df()
    if df.empty:
        print("No hay datos limpios para producir eventos.")
        return

    with open(EVENTS_PATH, "a", encoding="utf-8") as f:
        for _ in range(n):
            row = df.sample(1).iloc[0]
            action = random.choice(ACTIONS)
            amount = None
            if action in ("purchase", "refund"):
                # monto positivo; refunds negativos para distinguir
                base = float(row.get("amount", 0) or 0)
                if base <= 0:
                    base = round(abs(random.gauss(50, 15)), 2)
                amount = base if action == "purchase" else -base

            evt = {
                "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "user_id": int(row["user_id"]),
                "email": row["email"],
                "country": row.get("country") if isinstance(row.get("country"), str) else None,
                "action": action,
                "amount": amount,
            }
            f.write(json.dumps(evt, ensure_ascii=False) + "\n")
            time.sleep(0.01)  # pequeño pacing
    print(f"Se produjeron {n} eventos en {EVENTS_PATH}")


def consume(from_beginning: bool):
    # Crear archivo si no existe
    if not os.path.exists(EVENTS_PATH):
        open(EVENTS_PATH, "a", encoding="utf-8").close()

    with open(EVENTS_PATH, "r", encoding="utf-8") as f:
        if not from_beginning:
            f.seek(0, os.SEEK_END)  # comenzar a partir del final (modo tail)
        else:
            f.seek(0)

        try:
            while True:
                pos = f.tell()
                line = f.readline()
                if not line:
                    time.sleep(0.2)
                    f.seek(pos)
                else:
                    try:
                        evt = json.loads(line)
                        print(f"[{evt['ts']}] uid={evt['user_id']} {evt['action']} amount={evt.get('amount')}")
                    except json.JSONDecodeError:
                        # línea corrupta — continuar sin bloquear el consumidor
                        sys.stderr.write("Línea inválida en JSONL, se omite.\n")
        except KeyboardInterrupt:
            print("\nConsumidor detenido por el usuario.")


def main():
    parser = argparse.ArgumentParser(description="Productor/Consumidor de eventos JSONL")
    parser.add_argument("--produce", type=int, help="Cantidad de eventos a producir", default=None)
    parser.add_argument("--from-beginning", action="store_true", help="Leer desde el inicio del archivo")
    args = parser.parse_args()

    if args.produce is not None:
        produce(args.produce)
    else:
        consume(args.from_beginning)


if __name__ == "__main__":
    main()