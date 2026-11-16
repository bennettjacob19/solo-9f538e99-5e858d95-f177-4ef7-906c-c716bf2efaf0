FROM python:3.12-slim

WORKDIR /app
COPY server.py .

# 9000: 模拟升级后的接收端   9100: 故障控制接口（改字/慢读/断连）
EXPOSE 9000 9100
CMD ["python", "server.py"]
