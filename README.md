# HA API Bridge

Home Assistant 到 Home Assistant 的 Climate Entity Bridge。Remote HA 透過官方 WebSocket API 推送狀態；控制命令也經由同一條連線送回 Remote HA。

## 安裝與設定

- **Remote HA：** 不需安裝 Integration、不需修改設定檔，只需準備 URL 與 Long-Lived Access Token。
- **Client HA：** 透過 HACS Custom Repository 安裝此專案，於「設定 → 裝置與服務」新增 **HA API Bridge**，輸入 Remote HA URL 與 Token，再選擇要匯入的 Climate Entity。

Remote HA URL 可輸入 IP、主機名稱或完整的 `http://` / `https://` URL，例如 `192.168.1.118:8123`、`homeassistant.local:8123` 或 `https://ha.example.com`。未指定 scheme 時會自動使用 `http://`；明確輸入 HTTPS 時會保留 HTTPS。請輸入 Home Assistant 的 base URL，不要加上 `/api/websocket`。

目前只支援 `climate`。每個設定項目連線至一台 Remote HA；要加入另一台，請在 Client HA 再新增一個設定項目。

Bridge 不使用 REST、MQTT、資料庫或輪詢。Climate 控制命令送出後，Client HA 會等待 Remote HA 回報狀態，不會樂觀更新。

## 安全性

Token 只保存在 Client HA 的 ConfigEntry，不會放入 Entity state 或一般記錄訊息。請使用 HTTPS/WSS 連線跨越不受信任的網路。

## 開發

安裝 `requirements_test.txt` 後執行：

```sh
pytest -q
ruff check .
ruff format --check .
```
