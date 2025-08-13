# MarketQL – Cassandra Chatbot

Chatbot hỏi–đáp dữ liệu tài chính trên Cassandra bằng LangChain. Hỗ trợ 2 chế độ:
- **ReAct**: lý luận nhiều bước cho câu hỏi phức tạp
- **Tool-Calling**: nhanh, gọn cho câu hỏi trực tiếp

## 1) Cấu hình
Sao chép file `.env.example` thành `.env` và điền `OPENAI_API_KEY` thực tế.
Kiểm tra lại `TABLE_MINUTE` đúng tên bảng Spark đang ghi (vd `candles_1m`).

## 2) Chạy local
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

Lệnh nhanh khi chạy:
- `schema` xem sơ đồ keyspace/tables
- hỏi: "Get latest 10 records for BTCUSDT"
- đổi chế độ: sửa `AGENT_MODE` trong `.env` sang `tool`

## 3) Docker (tùy chọn)
- Tạo image từ `Dockerfile` và thêm service `chatbot` vào docker-compose của bạn.
