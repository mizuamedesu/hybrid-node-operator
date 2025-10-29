# Hybrid Node Operator

Kubernetesクラスタのオンプレミスノード障害時に、自動的にGCP VMを作成してフェイルオーバーするOperator。

## 機能

- オンプレミスノードの障害検知（NotReady監視）
- GCP VMの自動作成とクラスタへの参加
- オンプレミスノード復旧時の自動クリーンアップ

## ラベル設定

### 必須ラベル

#### `node-type`

| 値 | 付与先 | 意味 | 作用 |
|---|---|---|---|
| `onpremise` | オンプレミスノード | 監視対象 | NotReady時にGCP VM作成 |
| `gcp-permanent` | 永久的なGCPノード | Operator配置先 | Operator Podのスケジュール先 |
| `gcp-temporary` | 自動作成VM | フェイルオーバーノード | 自動付与、クリーンアップ対象 |

#### `hardware`

| 値 | 付与先 | 意味 | 作用 |
|---|---|---|---|
| `game-runner` | GameServer実行ノード | Kata Containers対応 | Agones nodeSelectorで使用 |

### オプションラベル

#### `node-location`

| 値 | 付与先 | 意味 | 作用 |
|---|---|---|---|
| `gcp` | GCP側全ノード | 場所識別 | 統計・監視・スケジューリング制約用 |

## セットアップ

### ラベル付与

```bash
# オンプレミスノード
kubectl label node uc-k8s4p node-type=onpremise hardware=game-runner

# GCPノード（Operator配置先）
kubectl label node gcp-worker node-type=gcp-permanent node-location=gcp
```

### ConfigMap編集

```bash
deploy/configmap.yaml
```

以下の値を設定:
- `GCP_PROJECT_ID`: GCPプロジェクトID
- `GCP_NETWORK`: VPC名
- `GCP_SUBNET`: サブネット名
- `GCP_IMAGE_NAME`: カスタムイメージ名
- `K8S_API_SERVER`: APIサーバーアドレス
- `GCP_NODE_COPY_LABELS`: コピーするラベルのカンマ区切りリスト

### GCP認証情報

```bash
kubectl create secret generic gcp-credentials \
  --from-file=key.json=/path/to/service-account-key.json
```