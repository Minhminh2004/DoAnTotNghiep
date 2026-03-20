from flask import Flask, render_template, request, jsonify
from faker import Faker
from datetime import datetime, timedelta
import pyodbc, random

app = Flask(__name__)
fake = Faker("vi_VN")

SERVER = r"DESKTOP-33J7KC7"
DATABASE = "DATN_NTM"

def get_conn():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
    )

def sql_type(t):
    return {
        "nvarchar": "NVARCHAR(255)",
        "varchar": "VARCHAR(255)",
        "date": "DATE",
        "int": "INT",
        "float": "FLOAT"
    }.get(t, "NVARCHAR(255)")

def fake_value(t, rule=""):
    if rule == "gender01":
        return random.choice([0, 1])

    if rule == "score10":
        return round(random.uniform(0, 10), 2) if t == "float" else random.randint(0, 10)

    if rule == "positive":
        return round(random.uniform(1, 100), 2) if t == "float" else random.randint(1, 100)

    if t in ["nvarchar", "varchar"]:
        return fake.name()

    if t == "int":
        return random.randint(1, 100)

    if t == "float":
        return round(random.uniform(1, 100), 2)

    if t == "date":
        base_date = datetime(2000, 1, 1)
        return (base_date + timedelta(days=random.randint(0, 9000))).date()

    return fake.word()

def get_fk_values(cur, ref_table, ref_column):
    cur.execute(f"SELECT [{ref_column}] FROM [{ref_table}]")
    rows = cur.fetchall()
    return [r[0] for r in rows]

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/generate", methods=["POST"])
def generate():
    data = request.json
    table = data.get("table_name")
    cols = data.get("columns", [])
    rows = int(data.get("row_count", 10))

    if not table or not cols:
        return jsonify({"success": False, "message": "Thiếu tên bảng hoặc cột."})

    try:
        conn = get_conn()
        cur = conn.cursor()

        defs = []
        pk_cols = []
        fk_defs = []
        check_defs = []

        for c in cols:
            col_name = c["name"]
            col_type = c["type"]
            col_def = f"[{col_name}] {sql_type(col_type)}"

            if c.get("notnull"):
                col_def += " NOT NULL"

            defs.append(col_def)

            if c.get("pk"):
                pk_cols.append(f"[{col_name}]")

            if c.get("fk") and c.get("ref_table") and c.get("ref_column"):
                fk_name = f"FK_{table}_{col_name}"
                fk_defs.append(
                    f"CONSTRAINT [{fk_name}] FOREIGN KEY ([{col_name}]) "
                    f"REFERENCES [{c['ref_table']}]([{c['ref_column']}])"
                )

            if c.get("rule") == "gender01":
                check_defs.append(
                    f"CONSTRAINT [CK_{table}_{col_name}_Gender] CHECK ([{col_name}] IN (0,1))"
                )

            if c.get("rule") == "score10":
                check_defs.append(
                    f"CONSTRAINT [CK_{table}_{col_name}_Score] CHECK ([{col_name}] BETWEEN 0 AND 10)"
                )

            if c.get("rule") == "positive":
                check_defs.append(
                    f"CONSTRAINT [CK_{table}_{col_name}_Positive] CHECK ([{col_name}] > 0)"
                )

        if pk_cols:
            defs.append(f"CONSTRAINT [PK_{table}] PRIMARY KEY ({', '.join(pk_cols)})")

        defs.extend(fk_defs)
        defs.extend(check_defs)

        create_sql = f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table}' AND xtype='U')
        CREATE TABLE [{table}] (
            {', '.join(defs)}
        )
        """

        cur.execute(create_sql)
        conn.commit()

        names = ", ".join(f"[{c['name']}]" for c in cols)
        qs = ", ".join("?" for _ in cols)
        sql = f"INSERT INTO [{table}] ({names}) VALUES ({qs})"

        for _ in range(rows):
            vals = []
            for c in cols:
                if c.get("fk") and c.get("ref_table") and c.get("ref_column"):
                    fk_values = get_fk_values(cur, c["ref_table"], c["ref_column"])
                    if not fk_values:
                        raise Exception(f"Bảng tham chiếu {c['ref_table']} chưa có dữ liệu.")
                    vals.append(random.choice(fk_values))
                else:
                    vals.append(fake_value(c["type"], c.get("rule", "")))

            cur.execute(sql, vals)

        conn.commit()
        conn.close()

        return jsonify({
            "success": True,
            "message": f"Đã sinh {rows} dòng dữ liệu vào bảng {table}."
        })

    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)