"""O2 모델 선정 실험 — 4개 축을 한 번에 돌리고 MLflow에 남긴다.

축 (발표_예상질문-답변.md 의 모델 비교 축 대조):
  longctx  컨텍스트 길이 × 프롬프트 버전 × 시나리오 × 모델   (지연·토큰·비용)
  tools    도구 호출 안정성 — 멀티턴 toolConfig            (비어 있던 축)
  risk     L1/L2/L3 판정 — seed_runbook.py 라벨과 대조      ("척도 정의 없다" 항목)
  judge    정확도 — 같은 셀의 두 출력을 Opus가 pairwise 판정 (라벨 없이 되는 축)

예산은 하드캡이다. 콜을 던지기 전에 최악 비용을 예약하고, 예약이 한도를
넘으면 아예 안 던진다. 그래서 BUDGET 을 절대 초과하지 않는다.

usage:  BURN_BUDGET=1000 python burn.py
        python burn.py --selftest
"""
import itertools, json, os, pathlib, re, sys, threading, time
from concurrent.futures import ThreadPoolExecutor

import boto3
import mlflow
from botocore.config import Config

REGION = "ap-northeast-2"
BUDGET = float(os.environ.get("BURN_BUDGET", "1100"))   # 세금·인프라·AI 전부 합친 하드캡

# ---- 버닝 전 이미 확정된 부담. spend.json 이 없으면 이 합으로 시작한다 ----
PROJECT_TO_DATE = 361.11   # CE 실측 8/12~8/29: Usage 356.96 + Tax 4.15
TAX_PENDING     = 30.50    # 8월 사용분 중 아직 안 찍힌 세금. AWS 서비스분은 9/1 에 붙는다
INFRA_DAILY     = 28.90    # 세금 포함 실측: 8/28 인프라 $26.24 × 1.10
INFRA_DAYS      = 1.5      # 버닝 시작 ~ 내일 오전 정리까지. 더 오래 켜둘 거면 이 값을 올려라
BASELINE = PROJECT_TO_DATE + TAX_PENDING + INFRA_DAILY * INFRA_DAYS
WORKERS = int(os.environ.get("BURN_WORKERS", "12"))
MAX_TOKENS = 16000
CHARS_PER_TOKEN = 2.0   # 8/29 실측(동일 200,000자): opus/sonnet 2.099, haiku 2.612 문자/토큰.
                        # 2.0 은 opus·sonnet 기준이라 목표보다 5% 적게 들어간다(넘치지 않음).
                        # ponytail: 토크나이저가 모델별 1.24배 다르다. 길이 축 분석은
                        # ctx_target 라벨이 아니라 MLflow 의 input_tokens 실측으로 해라.
TAX = 1.10              # 실측: 마켓플레이스분은 즉시, AWS 서비스분은 익월 1일에 붙는다
SAFETY = 1.20           # 예약용 입력 토큰 여유

ROOT = pathlib.Path(__file__).parent
STATE, CACHE, OUT = ROOT / "spend.json", ROOT / "bundles", ROOT / "outputs"
SEED_RUNBOOK = pathlib.Path(
    "/Users/jyc/Desktop/Workspace/projects/cj-cw-o2/O2-live-ai-ops/infra/06-agent/scripts/seed_runbook.py")

MODELS = {   # modelId, $/1M in, $/1M out, 길이 축
    "opus5":   ("global.anthropic.claude-opus-5",                   5.0, 25.0, [100_000, 400_000, 800_000]),
    "sonnet5": ("global.anthropic.claude-sonnet-5",                 2.0, 10.0, [100_000, 400_000, 800_000]),
    "haiku45": ("global.anthropic.claude-haiku-4-5-20251001-v1:0",  1.0,  5.0, [ 50_000, 100_000, 180_000]),
}

SOURCES = {
    "eks-cluster":  "/aws/eks/o2-eks/cluster",
    "chat-signal":  "/aws/lambda/o2-dev-chat-signal-worker",
    "agg":          "/aws/lambda/o2-agg",
    "dd-forwarder": "/aws/lambda/DatadogIntegration-ForwarderStack-78QECF-Forwarder-in1JkStpZ0rN",
    "canary":       "/aws/lambda/o2-canary",
    "glue-error":   "/aws-glue/jobs/error",
    "glue-logs":    "/aws-glue/jobs/logs-v2",
    "chat-adapter": "/aws/lambda/o2-dev-chat-candidate-source-adapter",
    "warm-api":     "/aws/lambda/o2-warm-api",
    "hot-api":      "/aws/lambda/o2-hot-api",
}
# ponytail: 작은 그룹(warm-api/hot-api ~0.4MB)은 800K 번들에서 같은 윈도우가 반복된다.
# 반복 구간은 길이 축 근거로만 쓰고 진단 품질 비교는 큰 그룹 6개로 해라.

PROMPTS = {
    "v1-baseline": "너는 O2 라이브 커머스 운영 AIOps 진단기다. 로그 윈도우를 읽고 "
                   "인시던트 후보, 원인 가설 상위 3개와 근거 로그 라인, 런북 후보와 "
                   "위험도(L1/L2/L3)를 내라. 근거가 부족하면 부족하다고 명시하고 "
                   "추측을 사실로 쓰지 마라.",
    "v2-evidence": "너는 O2 AIOps 진단기다. 규칙: 모든 주장에 로그 라인을 인용해 붙인다. "
                   "인용 못 붙이는 주장은 쓰지 않는다. 출력은 (1) 인시던트 후보와 시각 "
                   "(2) 원인 가설 3개 — 각각 인용과 반증 조건 (3) 런북 후보와 위험도 "
                   "(4) 미측정 항목 목록. 미측정을 안전으로 취급하지 마라.",
    "v3-terse":    "O2 AIOps 진단기. 로그 윈도우 진단. 출력 형식: incident / hypotheses(3, "
                   "각 근거 라인) / runbook_candidates(위험도 포함) / unknowns. "
                   "산문 금지, 항목만. 모르면 unknowns 에 넣어라.",
}

TOOLS = {"tools": [
    {"toolSpec": {"name": "get_metrics", "description": "서비스의 최근 지표를 조회한다.",
        "inputSchema": {"json": {"type": "object", "properties": {
            "service": {"type": "string"}, "window_s": {"type": "integer"}},
            "required": ["service", "window_s"]}}}},
    {"toolSpec": {"name": "get_logs", "description": "로그 그룹에서 패턴에 맞는 라인을 조회한다.",
        "inputSchema": {"json": {"type": "object", "properties": {
            "log_group": {"type": "string"}, "pattern": {"type": "string"},
            "limit": {"type": "integer"}}, "required": ["log_group", "pattern"]}}}},
    {"toolSpec": {"name": "lookup_runbook", "description": "rca_type 으로 active 런북 조치를 조회한다.",
        "inputSchema": {"json": {"type": "object", "properties": {
            "rca_type": {"type": "string"}}, "required": ["rca_type"]}}}},
]}
TOOL_REQUIRED = {"get_metrics": {"service", "window_s"},
                 "get_logs": {"log_group", "pattern"},
                 "lookup_runbook": {"rca_type"}}
TOOL_TURNS = 6

RISK_SYSTEM = ("너는 O2 운영 Guardrail 이다. 주어진 조치의 risk_level 을 L1/L2/L3 중 하나로 "
               "판정한다. 기준: 가역성, 영향 범위, 데이터/금전 영향 중 가장 높은 축. "
               "미측정이면 낮게 추정하지 말고 높게 잡아라. "
               "출력은 JSON 한 줄: {\"risk_level\":\"L?\",\"axis\":\"...\",\"why\":\"...\"}")

JUDGE_SYSTEM = ("너는 장애 진단 심사관이다. 같은 로그 윈도우에 대한 두 진단 A/B 를 비교한다. "
                "기준 순서: (1) 근거 인용의 사실성 (2) 원인 가설의 판별력 (3) 모르는 것을 "
                "모른다고 했는가 (4) 런북/위험도의 타당성. 분량과 문체는 보지 마라. "
                "출력은 JSON 한 줄: {\"winner\":\"A|B|tie\",\"why\":\"한 문장\"}")

PHASES = [("longctx", 0.48), ("tools", 0.21), ("risk", 0.10), ("judge", 0.21)]

_lock = threading.Lock()
_clients = {}
_spent = 0.0
_reserved = 0.0
_cap = 0.0          # 현재 페이즈 상한


def bedrock():
    if "bedrock" not in _clients:
        _clients["bedrock"] = boto3.client(
            "bedrock-runtime", region_name=REGION,
            config=Config(retries={"max_attempts": 8, "mode": "adaptive"}, read_timeout=900))
    return _clients["bedrock"]


def logs():
    return _clients.setdefault("logs", boto3.client("logs", region_name=REGION))


def mlf():
    return _clients.setdefault("mlf", mlflow.MlflowClient())


def cost(in_tok, out_tok, p_in, p_out):
    """세금 포함. spend.json 을 계정 총액으로 시딩해 쓰므로 단위를 맞춘다."""
    return (in_tok / 1e6 * p_in + out_tok / 1e6 * p_out) * TAX


def est_max(in_chars, model_key, max_out=MAX_TOKENS):
    """이 콜이 최악의 경우 얼마인가. 예약은 이 값으로 한다."""
    _, p_in, p_out, _ = MODELS[model_key]
    in_tok = in_chars / CHARS_PER_TOKEN * SAFETY
    return cost(in_tok, max_out, p_in, p_out)


def reserve(max_usd):
    """한도 안이면 예약하고 True. 넘으면 안 던진다."""
    global _reserved
    with _lock:
        if _spent + _reserved + max_usd > _cap:
            return False
        _reserved += max_usd
        return True


def settle(max_usd, actual_usd):
    global _spent, _reserved
    with _lock:
        _reserved -= max_usd
        _spent += actual_usd
        STATE.write_text(json.dumps({"usd": _spent}))
        return _spent


def call(model_key, system, messages, max_out=MAX_TOKENS, tool_config=None):
    """예약 → 호출 → 정산. 예약 실패면 None."""
    mid, p_in, p_out, _ = MODELS[model_key]
    in_chars = sum(len(b.get("text", "")) for m in messages for b in m["content"]) + len(system)
    budget_max = est_max(in_chars, model_key, max_out)
    if not reserve(budget_max):
        return None
    kw = dict(modelId=mid, system=[{"text": system}], messages=messages,
              inferenceConfig={"maxTokens": max_out})
    if tool_config:
        kw["toolConfig"] = tool_config
    try:
        try:
            r = bedrock().converse(**kw, additionalModelRequestFields={"thinking": {"type": "adaptive"}})
        except bedrock().exceptions.ValidationException:
            r = bedrock().converse(**kw)
        u = r["usage"]
        actual = cost(u["inputTokens"], u["outputTokens"], p_in, p_out)
    except Exception:
        settle(budget_max, 0.0)
        raise
    total = settle(budget_max, actual)
    return r, actual, total


def log_run(name, params, metrics, artifacts=()):
    run = mlf().create_run(EXP_ID, run_name=name)
    for k, v in params.items():
        mlf().log_param(run.info.run_id, k, v)
    for k, v in metrics.items():
        mlf().log_metric(run.info.run_id, k, v)
    for fname, text in artifacts:
        mlf().log_text(run.info.run_id, text, fname)
    mlf().set_terminated(run.info.run_id, "FINISHED")


def bundle(scenario, target_tokens):
    """실제 로그를 target_tokens 만큼 모아 캐시. 토큰 ~= 문자/CHARS_PER_TOKEN."""
    CACHE.mkdir(exist_ok=True)
    f = CACHE / f"{scenario}-{target_tokens}.txt"
    if f.exists():
        return f.read_text()
    need, chunks, got, token = int(target_tokens * CHARS_PER_TOKEN), [], 0, None
    while got < need:
        kw = {"logGroupName": SOURCES[scenario], "limit": 10000}
        if token:
            kw["nextToken"] = token
        r = logs().filter_log_events(**kw)
        events = r.get("events", [])
        if not events:
            break
        for e in events:
            chunks.append(e["message"])
            got += len(e["message"])
        token = r.get("nextToken")
        if not token:
            break
    text = "\n".join(chunks)
    if len(text) < need:   # 로그가 모자라면 같은 윈도우가 반복된다 (위 ponytail 주석)
        text = (text + "\n") * (need // max(len(text), 1) + 1)
    text = text[:need]
    tmp = f.with_suffix(f"{f.suffix}.{os.getpid()}.{threading.get_ident()}.tmp")
    tmp.write_text(text)
    tmp.replace(f)         # 원자적 교체 — 두 워커가 같은 번들을 동시에 만들 때 대비
    return text


def actions():
    """seed_runbook.py 에서 (action_id, risk_level) 실목록을 뽑는다."""
    s = SEED_RUNBOOK.read_text()
    ids = re.findall(r"['\"]action_id['\"]\s*:\s*['\"]([^'\"]+)", s)
    lv = re.findall(r"['\"]risk_level['\"]\s*:\s*['\"]([^'\"]+)", s)
    seen, out = set(), []
    for a, l in zip(ids, lv):
        if (a, l) not in seen:
            seen.add((a, l))
            out.append((a, l))
    return out


# ---------------- 페이즈 ----------------

def task_longctx(scenario, model_key, ctx, pv, seed):
    body = bundle(scenario, ctx)
    msg = [{"role": "user", "content": [{"text":
            f"<logs source={scenario}>\n{body}\n</logs>\n위 지시대로 진단하라."}]}]
    got = call(model_key, PROMPTS[pv], msg)
    if not got:
        return False
    r, usd, total, = got[0], got[1], got[2]
    u, text = r["usage"], r["output"]["message"]["content"][-1]["text"]
    OUT.mkdir(exist_ok=True)
    (OUT / f"{scenario}__{ctx}__{pv}__s{seed}__{model_key}.md").write_text(text)
    log_run(f"longctx-{scenario}-{model_key}-{ctx//1000}k-{pv}-s{seed}",
            {"axis": "longctx", "model": model_key, "scenario": scenario,
             "ctx_target": ctx, "prompt_version": pv, "seed": seed},
            {"input_tokens": u["inputTokens"], "output_tokens": u["outputTokens"],
             "usd": usd, "usd_cumulative": total},
            [("diagnosis.md", text)])
    print(f"longctx {scenario:13s} {model_key:8s} {ctx//1000:4d}k {pv:12s} s{seed} "
          f"in={u['inputTokens']:>7} ${usd:6.2f} 누적 ${total:8.2f}", flush=True)
    return True


def task_tools(scenario, model_key, seed):
    """멀티턴 도구 호출. 도구 결과는 실제 로그에서 만들어 준다."""
    body = bundle(scenario, 20_000)
    lines = body.split("\n")
    msgs = [{"role": "user", "content": [{"text":
             f"{scenario} 에서 장애 신호가 올라왔다. 도구로 증거를 모아 원인을 좁히고, "
             f"마지막에 런북 후보를 제시하라. 근거 없이 단정하지 마라."}]}]
    calls = bad_args = turns = 0
    usd_sum, total = 0.0, 0.0
    for turn in range(TOOL_TURNS):
        got = call(model_key, PROMPTS["v2-evidence"], msgs, max_out=4000, tool_config=TOOLS)
        if not got:
            return False
        r, usd, total = got
        usd_sum += usd
        turns = turn + 1
        out_msg = r["output"]["message"]
        msgs.append(out_msg)
        uses = [b["toolUse"] for b in out_msg["content"] if "toolUse" in b]
        if not uses:
            break
        results = []
        for tu in uses:
            calls += 1
            missing = TOOL_REQUIRED.get(tu["name"], set()) - set(tu["input"])
            if missing:
                bad_args += 1
            sample = "\n".join(lines[(calls * 37) % max(len(lines) - 40, 1):][:40])
            results.append({"toolResult": {"toolUseId": tu["toolUseId"],
                                           "content": [{"text": sample}]}})
        msgs.append({"role": "user", "content": results})
    log_run(f"tools-{scenario}-{model_key}-s{seed}",
            {"axis": "tools", "model": model_key, "scenario": scenario, "seed": seed},
            {"tool_calls": calls, "bad_args": bad_args, "turns": turns,
             "usd": usd_sum, "usd_cumulative": total})
    print(f"tools   {scenario:13s} {model_key:8s} turns={turns} calls={calls} "
          f"bad={bad_args} ${usd_sum:6.2f} 누적 ${total:8.2f}", flush=True)
    return True


def task_risk(action_id, label, model_key, seed):
    msg = [{"role": "user", "content": [{"text":
            f"조치 action_id: {action_id}\n"
            "이 조치의 risk_level 을 판정하라. 카탈로그의 기존 등급은 알려주지 않는다."}]}]
    got = call(model_key, RISK_SYSTEM, msg, max_out=2000)
    if not got:
        return False
    r, usd, total = got
    text = r["output"]["message"]["content"][-1]["text"]
    m = re.search(r'"risk_level"\s*:\s*"(L[123])"', text)
    verdict = m.group(1) if m else "PARSE_FAIL"
    log_run(f"risk-{action_id}-{model_key}-s{seed}",
            {"axis": "risk", "model": model_key, "action_id": action_id,
             "catalog_label": label, "verdict": verdict, "seed": seed},
            {"match": 1.0 if verdict == label else 0.0, "usd": usd, "usd_cumulative": total},
            [("verdict.md", text)])
    print(f"risk    {action_id:32s} {model_key:8s} {label}->{verdict:10s} "
          f"${usd:5.2f} 누적 ${total:8.2f}", flush=True)
    return True


def task_judge(cell, a_path, b_path):
    a, b = a_path.read_text(), b_path.read_text()
    msg = [{"role": "user", "content": [{"text":
            f"[셀 {cell}]\n\n<A>\n{a}\n</A>\n\n<B>\n{b}\n</B>\n어느 쪽이 나은가."}]}]
    got = call("opus5", JUDGE_SYSTEM, msg, max_out=2000)
    if not got:
        return False
    r, usd, total = got
    text = r["output"]["message"]["content"][-1]["text"]
    m = re.search(r'"winner"\s*:\s*"(A|B|tie)"', text)
    winner = m.group(1) if m else "PARSE_FAIL"
    a_model = a_path.stem.split("__")[-1]
    b_model = b_path.stem.split("__")[-1]
    log_run(f"judge-{cell}-{a_model}-vs-{b_model}",
            {"axis": "judge", "cell": cell, "a_model": a_model, "b_model": b_model,
             "winner": winner},
            {"a_win": 1.0 if winner == "A" else 0.0,
             "b_win": 1.0 if winner == "B" else 0.0,
             "usd": usd, "usd_cumulative": total},
            [("judgement.md", text)])
    print(f"judge   {cell:44s} {a_model} vs {b_model} -> {winner:10s} "
          f"${usd:5.2f} 누적 ${total:8.2f}", flush=True)
    return True


def gen_longctx():
    for seed in range(1, 100):
        for scenario in SOURCES:
            for pv in PROMPTS:
                for mk, (_, _, _, lengths) in MODELS.items():
                    for ctx in lengths:
                        yield task_longctx, (scenario, mk, ctx, pv, seed)


def gen_tools():
    for seed in range(1, 100):
        for scenario in SOURCES:
            for mk in MODELS:
                yield task_tools, (scenario, mk, seed)


def gen_risk():
    acts = actions()
    for seed in range(1, 11):   # 조치 12개 × 모델 3개 = 라운드당 36콜, ~$1.2.
                                # 자기 일관성 보려면 10회면 충분하고 남는 예산은 마무리로 간다.
        for action_id, label in acts:
            for mk in MODELS:
                yield task_risk, (action_id, label, mk, seed)


def gen_judge():
    """longctx 가 남긴 산출물을 같은 셀끼리 짝지어 비교."""
    files = sorted(OUT.glob("*.md"))
    cells = {}
    for f in files:
        cells.setdefault("__".join(f.stem.split("__")[:4]), []).append(f)
    pairs = [(c, a, b) for c, fs in cells.items() for a, b in itertools.combinations(sorted(fs), 2)]
    for cell, a, b in pairs:
        yield task_judge, (cell, a, b)


GENS = {"longctx": gen_longctx, "tools": gen_tools, "risk": gen_risk, "judge": gen_judge}


def run_phase(name, ceiling):
    global _cap
    _cap = ceiling
    print(f"\n=== {name} 페이즈 — 상한 ${ceiling:.2f} (현재 ${_spent:.2f}) ===", flush=True)
    gen, live = GENS[name](), set()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        exhausted = False
        while not exhausted or live:
            while not exhausted and len(live) < WORKERS:
                try:
                    fn, args = next(gen)
                except StopIteration:
                    exhausted = True
                    break
                live.add(pool.submit(fn, *args))
            if not live:
                break
            done = {f for f in live if f.done()} or {next(iter(live))}
            for f in list(done):
                live.discard(f)
                try:
                    if f.result() is False:      # 예약 거부 = 이 페이즈 예산 끝
                        exhausted = True
                except Exception as e:
                    print(f"  실패: {type(e).__name__}: {str(e)[:140]}", flush=True)
    print(f"=== {name} 종료 — 누적 ${_spent:.2f} ===", flush=True)


def main():
    global EXP_ID, _spent
    if STATE.exists():
        _spent = json.loads(STATE.read_text())["usd"]
    else:
        _spent = BASELINE
        STATE.write_text(json.dumps({"usd": _spent}))
    if _spent < BASELINE - 1:
        sys.exit(f"중단: spend.json 이 ${_spent:.2f} 로 베이스라인 ${BASELINE:.2f} 보다 낮다. "
                 f"오염된 상태다. spend.json 을 지우고 다시 돌려라.")
    print(f"버닝 전 부담 ${BASELINE:.2f} = 누적 ${PROJECT_TO_DATE} + 미청구 세금 ${TAX_PENDING} "
          f"+ 인프라 ${INFRA_DAILY}/일 × {INFRA_DAYS}일")
    mlflow.set_experiment("o2-model-selection")
    EXP_ID = mlflow.get_experiment_by_name("o2-model-selection").experiment_id
    start = _spent
    room = BUDGET - start
    print(f"한도 ${BUDGET} (하드캡) / 시작 ${start:.2f} / 태울 몫 ${room:.2f} / 동시성 {WORKERS}")
    if room <= 0:
        print("이미 한도. 할 일 없음."); return
    acc = start
    for name, frac in PHASES:
        acc += room * frac
        run_phase(name, acc)
    if BUDGET - _spent > 0.5:      # 남으면 제일 싼 콜로 마저 채운다
        print(f"\n=== 마무리 — 남은 ${BUDGET - _spent:.2f} ===", flush=True)
        run_phase("longctx", BUDGET)
    print(f"\n최종 누적 ${_spent:.2f} / 한도 ${BUDGET}")


def selftest():
    global _cap, _spent, _reserved, STATE
    real_state, STATE = STATE, ROOT / ".selftest-spend.json"   # 테스트가 실제 카운터를 건드리면 안 된다
    assert abs(cost(400_000, 8_000, 5.0, 25.0) - 2.20 * TAX) < 1e-9
    assert abs(cost(180_000, 8_000, 1.0, 5.0) - 0.22 * TAX) < 1e-9
    _cap, _spent, _reserved = 10.0, 9.0, 0.0
    assert reserve(0.5) is True and _reserved == 0.5
    assert reserve(0.6) is False, "한도 넘는 예약은 거부돼야 한다"
    assert settle(0.5, 0.2) == 9.2 and _reserved == 0.0
    _cap, _spent, _reserved = 0.0, 0.0, 0.0
    STATE.unlink(missing_ok=True)
    STATE = real_state
    assert not STATE.exists() or json.loads(STATE.read_text())["usd"] >= BASELINE - 1, \
        "selftest 가 실제 spend.json 을 오염시켰다"
    assert abs(BASELINE - 434.96) < 0.01, f"베이스라인 합계 어긋남: {BASELINE}"
    a = actions()
    assert len(a) >= 12, f"런북 조치 파싱 실패: {len(a)}"
    assert ("switch_pg_provider", "L3") in a
    print(f"selftest ok — 조치 {len(a)}개, 하드캡 동작 확인")


if __name__ == "__main__":
    selftest() if "--selftest" in sys.argv else main()
