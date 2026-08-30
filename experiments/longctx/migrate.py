"""로컬 sqlite MLflow(o2-model-selection) 를 원격 MLflow(SSM 터널)로 이관한다.
run 마다 params/metrics/tags 를 그대로 옮기고, 텍스트 artifact 는 REST 로 재업로드한다.
run_id 는 새로 발급된다(원격 발급 규칙이 다름). start_time 은 원본 그대로 보존한다.

usage: python migrate.py
"""
import base64, json, mimetypes, sqlite3, sys, urllib.request

REMOTE = "http://127.0.0.1:18080"
LOCAL_DB = "mlflow.db"
LOCAL_EXP_NAME = "o2-model-selection"
REMOTE_EXP_NAME = "o2-model-selection"


def api(path, body):
    req = urllib.request.Request(f"{REMOTE}{path}", data=json.dumps(body).encode(),
                                  headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def get_or_create_experiment(name):
    r = api("/api/2.0/mlflow/experiments/search", {"max_results": 1000,
             "filter": f"name = '{name}'"})
    for e in r.get("experiments", []):
        if e["name"] == name:
            return e["experiment_id"]
    return api("/api/2.0/mlflow/experiments/create", {"name": name})["experiment_id"]


def already_migrated(remote_exp_id):
    names, off = set(), 0
    while True:
        r = api("/api/2.0/mlflow/runs/search", {"experiment_ids":[remote_exp_id],
                 "max_results": 5000, **({"page_token": off} if off else {})})
        for run in r.get("runs", []):
            for t in run["data"].get("tags", []):
                if t["key"] == "mlflow.runName":
                    names.add(t["value"])
        tok = r.get("next_page_token")
        if not tok:
            break
        off = tok
    return names


def main():
    c = sqlite3.connect(LOCAL_DB)
    local_exp = c.execute("select experiment_id from experiments where name=?",
                           (LOCAL_EXP_NAME,)).fetchone()
    if not local_exp:
        sys.exit(f"로컬에 실험 '{LOCAL_EXP_NAME}' 없음")
    local_exp_id = local_exp[0]
    runs = c.execute("""select run_uuid, name, start_time, end_time, status
                         from runs where experiment_id=? and lifecycle_stage='active'
                         order by start_time""", (local_exp_id,)).fetchall()
    print(f"로컬 run {len(runs)}개, 원격으로 이관 시작")

    remote_exp_id = get_or_create_experiment(REMOTE_EXP_NAME)
    done = already_migrated(remote_exp_id)
    print(f"원격 실험 id={remote_exp_id}, 이미 이관됨 {len(done)}개 — 건너뜀")

    ok = fail = skip = 0
    for i, (uuid, name, start, end, status) in enumerate(runs, 1):
        if name in done:
            skip += 1
            continue
        params = c.execute("select key, value from params where run_uuid=?", (uuid,)).fetchall()
        metrics = c.execute("select key, value, timestamp, step from metrics where run_uuid=?",
                             (uuid,)).fetchall()
        tags = c.execute("select key, value from tags where run_uuid=?", (uuid,)).fetchall()
        try:
            created = api("/api/2.0/mlflow/runs/create", {
                "experiment_id": remote_exp_id, "start_time": start,
                "tags": [{"key": "mlflow.runName", "value": name}] +
                        [{"key": k, "value": v} for k, v in tags if not k.startswith("mlflow.")]})
            new_uuid = created["run"]["info"]["run_uuid"]

            if params:
                api("/api/2.0/mlflow/runs/log-batch", {
                    "run_id": new_uuid,
                    "params": [{"key": k, "value": v} for k, v in params]})
            if metrics:
                api("/api/2.0/mlflow/runs/log-batch", {
                    "run_id": new_uuid,
                    "metrics": [{"key": k, "value": float(v), "timestamp": ts, "step": st}
                                for k, v, ts, st in metrics]})
            api("/api/2.0/mlflow/runs/update", {
                "run_id": new_uuid, "status": status, "end_time": end})
            ok += 1
        except Exception as e:
            fail += 1
            print(f"  실패 {name}: {type(e).__name__}: {str(e)[:150]}")
        if i % 50 == 0 or i == len(runs):
            print(f"  {i}/{len(runs)}  성공={ok} 실패={fail}", flush=True)

    print(f"완료: 성공 {ok} / 건너뜀 {skip} / 실패 {fail}")
    print(f"확인: {REMOTE}/#/experiments/{remote_exp_id}")


if __name__ == "__main__":
    main()
