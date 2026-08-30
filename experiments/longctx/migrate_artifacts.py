"""migrate.py 가 run(params/metrics)만 옮기고 artifact(diagnosis.md 등)는 빠뜨렸다.
이 스크립트가 그 구멍을 메운다 — run_name 으로 로컬 uuid ↔ 원격 uuid 를 매칭해
로컬 artifacts/ 안의 파일을 원격 mlflow-artifacts 프록시로 PUT 한다.

usage: python migrate_artifacts.py
"""
import pathlib, sqlite3, sys, urllib.request

REMOTE = "http://127.0.0.1:18080"
LOCAL_DB = "mlflow.db"
LOCAL_EXP_NAME = "o2-model-selection"
REMOTE_EXP_NAME = "o2-model-selection"
MLRUNS = pathlib.Path("mlruns")


def get_json(path):
    with urllib.request.urlopen(f"{REMOTE}{path}", timeout=30) as r:
        import json
        return json.loads(r.read())


def post_json(path, body):
    import json
    req = urllib.request.Request(f"{REMOTE}{path}", data=json.dumps(body).encode(),
                                  headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def put_artifact(exp_id, run_id, filename, data):
    req = urllib.request.Request(
        f"{REMOTE}/api/2.0/mlflow-artifacts/artifacts/{exp_id}/{run_id}/artifacts/{filename}",
        data=data, headers={"Content-Type": "application/octet-stream"}, method="PUT")
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status


def remote_name_to_uuid(remote_exp_id):
    out, token = {}, None
    while True:
        body = {"experiment_ids": [remote_exp_id], "max_results": 5000}
        if token:
            body["page_token"] = token
        r = post_json("/api/2.0/mlflow/runs/search", body)
        for run in r.get("runs", []):
            name = next((t["value"] for t in run["data"].get("tags", [])
                         if t["key"] == "mlflow.runName"), None)
            if name:
                out[name] = run["info"]["run_uuid"]
        token = r.get("next_page_token")
        if not token:
            break
    return out


def main():
    c = sqlite3.connect(LOCAL_DB)
    local_exp = c.execute("select experiment_id from experiments where name=?",
                           (LOCAL_EXP_NAME,)).fetchone()
    if not local_exp:
        sys.exit(f"로컬에 실험 '{LOCAL_EXP_NAME}' 없음")
    local_exp_id = local_exp[0]

    exps = post_json("/api/2.0/mlflow/experiments/search",
                      {"max_results": 1000, "filter": f"name = '{REMOTE_EXP_NAME}'"})
    remote_exp_id = next(e["experiment_id"] for e in exps["experiments"]
                          if e["name"] == REMOTE_EXP_NAME)

    print("원격 run_name -> uuid 매핑 로드 중...")
    remote_map = remote_name_to_uuid(remote_exp_id)
    print(f"원격 run {len(remote_map)}개")

    runs = c.execute("select run_uuid, name from runs where experiment_id=? and lifecycle_stage='active'",
                      (local_exp_id,)).fetchall()

    ok = skip = miss = fail = 0
    for i, (local_uuid, name) in enumerate(runs, 1):
        adir = MLRUNS / local_exp_id / local_uuid / "artifacts"
        if not adir.is_dir():
            skip += 1
            continue
        remote_uuid = remote_map.get(name)
        if not remote_uuid:
            miss += 1
            continue
        for f in adir.iterdir():
            if not f.is_file():
                continue
            try:
                put_artifact(remote_exp_id, remote_uuid, f.name, f.read_bytes())
                ok += 1
            except Exception as e:
                fail += 1
                print(f"  실패 {name}/{f.name}: {type(e).__name__}: {str(e)[:120]}")
        if i % 100 == 0 or i == len(runs):
            print(f"  {i}/{len(runs)}  업로드={ok} 매칭실패={miss} 실패={fail}", flush=True)

    print(f"완료: 업로드 {ok} / artifact없음 {skip} / run매칭실패 {miss} / 실패 {fail}")


if __name__ == "__main__":
    main()
