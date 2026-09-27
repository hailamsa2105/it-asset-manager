import os
import sys
import socket
import asyncio
import webbrowser
from datetime import datetime
from typing import List, Optional
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Text
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# ==============================================================================
# 1. CẤU HÌNH ĐƯỜNG DẪN VÀ CƠ SỞ DỮ LIỆU SQLITE
# ==============================================================================
# Xác định thư mục chứa file chạy (đảm bảo tạo file database đúng chỗ kể cả khi đóng gói .exe/.app)
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_PATH = os.path.join(BASE_DIR, "it_assets.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ==============================================================================
# 2. KHAI BÁO BẢNG DỮ LIỆU (DATABASE MODELS)
# ==============================================================================
class Device(Base):
    __tablename__ = "devices"
    id = Column(Integer, primary_key=True, index=True)
    asset_code = Column(String, unique=True, index=True)   # Mã tài sản (VD: CAM-01, PC-KT01)
    name = Column(String, nullable=False)                  # Tên thiết bị
    device_type = Column(String)                           # Phân loại: Camera, PC, Máy in, Switch...
    ip_address = Column(String, nullable=True)             # Địa chỉ IP nội bộ
    location_name = Column(String, default="Văn phòng")     # Vị trí lắp đặt
    initial_value = Column(Float, default=0.0)             # Giá trị tài sản (VNĐ)
    current_status = Column(String, default="offline")     # online hoặc offline
    last_ping = Column(DateTime, default=datetime.utcnow)

class MaintenanceLog(Base):
    __tablename__ = "maintenance_logs"
    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(Integer, ForeignKey("devices.id"))
    technician = Column(String)                            # Kỹ thuật viên phụ trách
    replaced_parts = Column(Text)                          # Phụ tùng / linh kiện thay thế
    cost = Column(Float, default=0.0)                      # Chi phí sửa chữa
    maintenance_date = Column(DateTime, default=datetime.utcnow)

# Tự động tạo bảng trong SQLite nếu chưa tồn tại
Base.metadata.create_all(bind=engine)

# ==============================================================================
# 3. ĐỊNH NGHĨA DỮ LIỆU ĐẦU VÀO (PYDANTIC SCHEMAS)
# ==============================================================================
class DeviceCreate(BaseModel):
    asset_code: str
    name: str
    device_type: str
    ip_address: Optional[str] = None
    location_name: Optional[str] = "Văn phòng"
    initial_value: Optional[float] = 0.0

class MaintenanceCreate(BaseModel):
    device_id: int
    technician: str
    replaced_parts: str
    cost: float

# ==============================================================================
# 4. TIẾN TRÌNH GIÁM SÁT MẠNG TỰ ĐỘNG (BACKGROUND MONITOR)
# ==============================================================================
def check_device_online(ip: str, timeout: float = 1.0) -> bool:
    """Kiểm tra thiết bị hoạt động qua kết nối socket cổng dịch vụ hoặc Ping hệ điều hành"""
    if not ip or ip.strip() == "":
        return False
    
    # 1. Quét nhanh các cổng phổ biến (Camera: 80, 554; Máy in/PC: 80, 445, 9100)
    for port in [80, 445, 554, 9100, 8080]:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            res = sock.connect_ex((ip.strip(), port))
            sock.close()
            if res == 0:
                return True
        except Exception:
            pass

    # 2. Cơ chế dự phòng: Gửi 1 gói tin Ping tiêu chuẩn của hệ điều hành
    param = "-n 1 -w 500" if sys.platform.startswith("win") else "-c 1 -W 1"
    null_dev = "nul" if sys.platform.startswith("win") else "/dev/null"
    return os.system(f"ping {param} {ip.strip()} > {null_dev} 2>&1") == 0

async def background_ping_task():
    """Tiến trình ngầm kiểm tra định kỳ mỗi 30 giây"""
    while True:
        db = SessionLocal()
        try:
            devices = db.query(Device).all()
            for dev in devices:
                if dev.ip_address:
                    is_alive = check_device_online(dev.ip_address)
                    dev.current_status = "online" if is_alive else "offline"
                    dev.last_ping = datetime.utcnow()
            db.commit()
        except Exception as err:
            print(f"Lỗi kiểm tra mạng: {err}")
        finally:
            db.close()
        await asyncio.sleep(30)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Khởi động tiến trình ngầm kiểm tra mạng
    monitor_task = asyncio.create_task(background_ping_task())
    # Tự động mở trình duyệt sau khi máy chủ khởi động thành công
    asyncio.get_event_loop().call_later(1.5, lambda: webbrowser.open("http://localhost:8000"))
    yield
    monitor_task.cancel()

app = FastAPI(title="IT Asset Sentinel", lifespan=lifespan)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ==============================================================================
# 5. CÁC ĐƯỜNG DẪN DỮ LIỆU (RESTFUL APIS)
# ==============================================================================
@app.get("/api/devices")
def get_devices(db: Session = Depends(get_db)):
    return db.query(Device).all()

@app.post("/api/devices")
def create_device(item: DeviceCreate, db: Session = Depends(get_db)):
    device = Device(**item.model_dump())
    db.add(device)
    db.commit()
    db.refresh(device)
    return device

@app.get("/api/logs")
def get_logs(db: Session = Depends(get_db)):
    return db.query(MaintenanceLog).order_by(MaintenanceLog.maintenance_date.desc()).all()

@app.post("/api/logs")
def create_log(item: MaintenanceCreate, db: Session = Depends(get_db)):
    log = MaintenanceLog(**item.model_dump())
    db.add(log)
    db.commit()
    db.refresh(log)
    return log

# ==============================================================================
# 6. GIAO DIỆN WEB TỔNG QUAN (EMBEDDED DASHBOARD)
# ==============================================================================
@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    return """
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>IT Asset & Sentinel Monitor</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    </head>
    <body class="bg-slate-950 text-slate-100 min-h-screen font-sans antialiased">
        <div class="max-w-7xl mx-auto p-4 sm:p-6">
            <!-- Thanh tiêu đề -->
            <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center pb-6 border-b border-slate-800 gap-4">
                <div>
                    <h1 class="text-2xl font-bold text-white flex items-center gap-3">
                        <i class="fa-solid fa-server text-indigo-500"></i> Bảng Điều Khiển Quản Trị CNTT
                    </h1>
                    <p class="text-xs text-slate-400 mt-1">Giám sát trạng thái thiết bị thời gian thực & Lịch sử bảo trì</p>
                </div>
                <div class="flex gap-2">
                    <button onclick="openModal('deviceModal')" class="bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-2 rounded-xl text-sm font-medium transition flex items-center gap-2">
                        <i class="fa-solid fa-plus"></i> Thêm Thiết Bị
                    </button>
                    <button onclick="openModal('logModal')" class="bg-slate-800 hover:bg-slate-700 text-slate-200 px-4 py-2 rounded-xl text-sm font-medium transition flex items-center gap-2">
                        <i class="fa-solid fa-wrench"></i> Ghi Bảo Trì
                    </button>
                </div>
            </div>

            <!-- Thống kê trạng thái -->
            <div class="grid grid-cols-2 md:grid-cols-4 gap-4 my-6">
                <div class="bg-slate-900 border border-slate-800 p-4 rounded-2xl">
                    <span class="text-xs text-slate-400 block mb-1">Tổng thiết bị</span>
                    <span id="stat-total" class="text-2xl font-bold text-white">0</span>
                </div>
                <div class="bg-slate-900 border border-slate-800 p-4 rounded-2xl">
                    <span class="text-xs text-emerald-400 block mb-1">Đang trực tuyến</span>
                    <span id="stat-online" class="text-2xl font-bold text-emerald-400">0</span>
                </div>
                <div class="bg-slate-900 border border-slate-800 p-4 rounded-2xl">
                    <span class="text-xs text-rose-400 block mb-1">Mất kết nối</span>
                    <span id="stat-offline" class="text-2xl font-bold text-rose-400">0</span>
                </div>
                <div class="bg-slate-900 border border-slate-800 p-4 rounded-2xl">
                    <span class="text-xs text-amber-400 block mb-1">Tổng nguyên giá</span>
                    <span id="stat-value" class="text-2xl font-bold text-amber-300">0 đ</span>
                </div>
            </div>

            <!-- Danh sách thiết bị -->
            <h2 class="text-lg font-semibold text-slate-200 mb-4 flex items-center gap-2">
                <i class="fa-solid fa-network-wired text-slate-400 text-sm"></i> Hiện Trạng Thiết Bị
            </h2>
            <div id="device-grid" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4"></div>
        </div>

        <!-- Cửa sổ Popup: Thêm thiết bị -->
        <div id="deviceModal" class="hidden fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center p-4">
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 w-full max-w-md">
                <h3 class="text-lg font-bold text-white mb-4">Thêm thiết bị mới</h3>
                <div class="space-y-3">
                    <input id="dev-code" placeholder="Mã tài sản (VD: CAM-001, PC-KT01)" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-sm text-white">
                    <input id="dev-name" placeholder="Tên thiết bị (VD: Camera Cửa Chính)" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-sm text-white">
                    <input id="dev-type" placeholder="Loại (Camera, Máy tính, Máy in...)" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-sm text-white">
                    <input id="dev-ip" placeholder="Địa chỉ IP (VD: 192.168.1.50)" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-sm text-white">
                    <input id="dev-location" placeholder="Khu vực (VD: Tầng 1, Kho A)" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-sm text-white">
                    <input id="dev-val" type="number" placeholder="Nguyên giá (VNĐ)" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-sm text-white">
                </div>
                <div class="flex justify-end gap-2 mt-6">
                    <button onclick="closeModal('deviceModal')" class="px-4 py-2 bg-slate-800 rounded-lg text-sm text-slate-300">Hủy</button>
                    <button onclick="submitDevice()" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 rounded-lg text-sm font-medium text-white">Lưu Thiết Bị</button>
                </div>
            </div>
        </div>

        <!-- Cửa sổ Popup: Ghi bảo trì -->
        <div id="logModal" class="hidden fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center p-4">
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 w-full max-w-md">
                <h3 class="text-lg font-bold text-white mb-4">Ghi nhận bảo trì & thay thế</h3>
                <div class="space-y-3">
                    <select id="log-device-id" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-sm text-slate-200"></select>
                    <input id="log-tech" placeholder="Kỹ thuật viên thực hiện" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-sm text-white">
                    <input id="log-parts" placeholder="Linh kiện thay thế (VD: Nguồn 12V, Ram 8GB)" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-sm text-white">
                    <input id="log-cost" type="number" placeholder="Chi phí sửa chữa (VNĐ)" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-sm text-white">
                </div>
                <div class="flex justify-end gap-2 mt-6">
                    <button onclick="closeModal('logModal')" class="px-4 py-2 bg-slate-800 rounded-lg text-sm text-slate-300">Hủy</button>
                    <button onclick="submitLog()" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 rounded-lg text-sm font-medium text-white">Lưu Nhật Ký</button>
                </div>
            </div>
        </div>

        <script>
            let currentDevices = [];

            async function refreshUI() {
                try {
                    const res = await fetch('/api/devices');
                    currentDevices = await res.json();
                    
                    let online = 0, totalVal = 0;
                    const grid = document.getElementById('device-grid');
                    grid.innerHTML = '';

                    currentDevices.forEach(d => {
                        if (d.current_status === 'online') online++;
                        totalVal += (d.initial_value || 0);

                        const isOnline = d.current_status === 'online';
                        const card = document.createElement('div');
                        card.className = "bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-col justify-between hover:border-slate-700 transition";
                        card.innerHTML = `
                            <div>
                                <div class="flex justify-between items-start mb-3">
                                    <div>
                                        <span class="text-xs font-mono px-2 py-0.5 rounded bg-slate-800 text-indigo-400 font-semibold">${d.asset_code}</span>
                                        <h3 class="text-base font-semibold text-white mt-1.5">${d.name}</h3>
                                    </div>
                                    <span class="flex items-center gap-1.5 text-xs font-medium px-2.5 py-1 rounded-full ${isOnline ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20' : 'bg-rose-500/10 text-rose-400 border border-rose-500/20'}">
                                        <span class="w-2 h-2 rounded-full ${isOnline ? 'bg-emerald-400 animate-pulse' : 'bg-rose-500'}"></span>
                                        ${isOnline ? 'ONLINE' : 'OFFLINE'}
                                    </span>
                                </div>
                                <div class="space-y-1 text-xs text-slate-400 mt-2">
                                    <p><i class="fa-solid fa-location-dot w-4"></i> Vị trí: ${d.location_name}</p>
                                    <p><i class="fa-solid fa-network-wired w-4"></i> IP: ${d.ip_address || 'Chưa thiết lập'}</p>
                                    <p><i class="fa-solid fa-layer-group w-4"></i> Phân loại: ${d.device_type}</p>
                                </div>
                            </div>
                            <div class="border-t border-slate-800/80 mt-4 pt-3 flex justify-between items-center text-xs">
                                <span class="text-slate-500">Nguyên giá:</span>
                                <span class="text-amber-300 font-medium">${Number(d.initial_value).toLocaleString('vi-VN')} đ</span>
                            </div>
                        `;
                        grid.appendChild(card);
                    });

                    document.getElementById('stat-total').innerText = currentDevices.length;
                    document.getElementById('stat-online').innerText = online;
                    document.getElementById('stat-offline').innerText = currentDevices.length - online;
                    document.getElementById('stat-value').innerText = Number(totalVal).toLocaleString('vi-VN') + ' đ';

                    const devSelect = document.getElementById('log-device-id');
                    devSelect.innerHTML = currentDevices.map(d => `<option value="${d.id}">${d.asset_code} - ${d.name}</option>`).join('');
                } catch(e) {
                    console.error("Lỗi cập nhật dữ liệu:", e);
                }
            }

            function openModal(id) { document.getElementById(id).classList.remove('hidden'); }
            function closeModal(id) { document.getElementById(id).classList.add('hidden'); }

            async function submitDevice() {
                const payload = {
                    asset_code: document.getElementById('dev-code').value,
                    name: document.getElementById('dev-name').value,
                    device_type: document.getElementById('dev-type').value,
                    ip_address: document.getElementById('dev-ip').value,
                    location_name: document.getElementById('dev-location').value,
                    initial_value: parseFloat(document.getElementById('dev-val').value) || 0
                };
                await fetch('/api/devices', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(payload)
                });
                closeModal('deviceModal');
                refreshUI();
            }

            async function submitLog() {
                const payload = {
                    device_id: parseInt(document.getElementById('log-device-id').value),
                    technician: document.getElementById('log-tech').value,
                    replaced_parts: document.getElementById('log-parts').value,
                    cost: parseFloat(document.getElementById('log-cost').value) || 0
                };
                await fetch('/api/logs', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(payload)
                });
                closeModal('logModal');
                alert("Đã lưu lịch sử bảo dưỡng thành công!");
            }

            refreshUI();
            setInterval(refreshUI, 10000); // Tự động làm mới dữ liệu sau mỗi 10 giây
        </script>
    </body>
    </html>
    """

# ==============================================================================
# 7. KHỞI ĐỘNG PHẦN MỀM
# ==============================================================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
