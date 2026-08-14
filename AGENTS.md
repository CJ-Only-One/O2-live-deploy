# 작업 전에

**Argo CD가 감시하는 저장소.** 여기 매니페스트가 곧 클러스터의 현재 상태다.

## 손대지 말 것

**이미지 태그.** `O2-live-ai-ops` 의 `app.yml` 이 `yq` 로 갱신한다.
손으로 고치면 다음 배포에서 덮어써진다. 배포하려면 앱 저장소에 코드를 머지한다.

## 파일명 규약

`<service>-deployment.yaml` / `<service>-service.yaml`.
CI 가 이 경로로 태그를 찾으므로 어긋나면 **갱신이 조용히 건너뛰어진다.**

## 새 서비스를 붙일 때

```yaml
spec:
  template:
    spec:
      serviceAccountName: <service>     # 04-platform 의 app_service_accounts 와 같아야 함
      containers:
        - envFrom:
            - configMapRef: { name: o2-data }   # DB·Valkey·SQS 주소
            - secretRef:    { name: o2-db }     # DB_PASSWORD
```

접속 정보를 **여기 직접 적지 않는다.** 데이터 스택을 다시 만들면 바뀌는데,
박아두면 그때 조용히 어긋난다. 둘 다 `infra/04-platform` 이 만든다.

`serviceAccountName` 이 목록에 없으면 파드는 정상적으로 뜨고 **AWS 호출에서만**
실패한다. 두 곳을 함께 고친다.

## 그 밖에

- `main` 은 PR 필수. 태그 갱신 봇만 우회한다
- 고치면 `kubeconform -strict` 가 검사한다. 스키마에 없는 필드는 오류다
- Argo 반영은 최대 180초. 되돌리려면 `git revert`
- HPA·KEDA 를 붙이는 서비스는 `replicas` 필드를 **제거**한다. `selfHeal` 과 충돌한다

## 더 읽을 것

설계·결정·계약은 전부 앱 저장소에 있다.

| 알고 싶은 것 | 위치 (`O2-live-ai-ops`) |
|---|---|
| 규약 요약, 문서 지도 | `CLAUDE.md` |
| 왜 이렇게 했나 | `docs/decisions.md` (인덱스에서 골라 그 절만) |
| API·WebSocket 규격 | `docs/contracts.md` |
