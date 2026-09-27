import os
import sys
import time
import socket
import asyncio
import webbrowser
from datetime import datetime
from typing import List, Optional
from contextlib import asynccontextmanager

# Khắc phục lỗi luồng console khi chạy chế độ ngầm trên Windows
if sys.stdout is None:
    sys.stdout = open(os.devnull, "w", encoding="utf-8")
if sys.stderr is None:
    sys.stderr = open(os.devnull, "w", encoding="utf-8")
if sys.stdin is None:
    sys.stdin = open(os.devnull, "r", encoding="utf-8")

import uvicorn
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Text
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship

# Khởi tạo CSDL SQLite cục bộ
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
# 1. DATABASE MODELS (QUAN HỆ NHÓM, THIẾT BỊ, LỊCH SỬ SENSOR & BẢO TRÌ)
# ==============================================================================
class Location(Base):
    __tablename__ = "locations"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)
    description = Column(String(255), default="")

class Device(Base):
    __tablename__ = "devices"
    id = Column(Integer, primary_key=True, index=True)
    asset_code = Column(String(50), unique=True, index=True)
    name = Column(String(150), nullable=False)
    device_type = Column(String(50))                      # Camera, Máy tính, Máy in, Server, Switch...
    ip_address = Column(String(45), nullable=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)
    initial_value = Column(Float, default=0.0)             # Nguyên giá tài sản
    status = Column(String(20), default="down")            # up, warning, down, paused
    latency_ms = Column(Float, default=0.0)                # Độ trễ ping thời gian thực
    last_checked = Column(DateTime, default=datetime.utcnow)

class PingHistory(Base):
    __tablename__ = "ping_history"
    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(Integer, ForeignKey("devices.id", ondelete="CASCADE"))
    latency_ms = Column(Float, default=0.0)
    status = Column(String(20))
    recorded_at = Column(DateTime, default=datetime.utcnow)

class MaintenanceLog(Base):
    __tablename__ = "maintenance_logs"
    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(Integer, ForeignKey("devices.id", ondelete="CASCADE"))
    technician = Column(String(100))
    replaced_parts = Column(Text)
    cost = Column(Float, default=0.0)
    maintenance_date = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

# ==============================================================================
# 2. KHỞI TẠO CÁC NHÓM MẶC ĐỊNH NẾU CHƯA CÓ
# ==============================================================================
def init_default_data():
    db = SessionLocal()
    try:
        if db.query(Location).count() == 0:
            default_locs = ["Phòng Server & Mạng", "Văn phòng Tầng 1", "Phòng Kế toán", "Kho vận & Bãi xe"]
            for name in default_locs:
                db.add(Location(name=name))
            db.commit()
    finally:
        db.close()

init_default_data()

# ==============================================================================
# 3. SCHEMAS
# ==============================================================================
class DeviceCreate(BaseModel):
    asset_code: str
    name: str
    device_type: str
    ip_address: Optional[str] = None
    location_id: int
    initial_value: Optional[float] = 0.0

class DeviceUpdate(BaseModel):
    asset_code: str
    name: str
    device_type: str
    ip_address: Optional[str] = None
    location_id: int
    initial_value: Optional[float] = 0.0

class DeviceMove(BaseModel):
    location_id: int

class MaintenanceCreate(BaseModel):
    device_id: int
    technician: str
    replaced_parts: str
    cost: float

class LocationCreate(BaseModel):
    name: str
    description: Optional[str] = ""

# ==============================================================================
# 4. TIẾN TRÌNH SENSOR ĐO ĐỘ TRỄ (PRTG ENGINE LOGIC)
# ==============================================================================
def ping_measure(ip: str, timeout: float = 1.2) -> tuple[str, float]:
    """Kiểm tra phản hồi mạng và đo chính xác độ trễ miligiây"""
    if not ip or not ip.strip():
        return "down", 0.0
    target = ip.strip()

    # Thử đo qua kết nối cổng mạng TCP
    for port in [80, 445, 554, 9100, 22, 8080]:
        try:
            start_t = time.perf_counter()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            res = s.connect_ex((target, port))
            s.close()
            elapsed_ms = round((time.perf_counter() - start_t) * 1000, 1)
            if res == 0:
                status = "warning" if elapsed_ms > 120 else "up"
                return status, elapsed_ms
        except Exception:
            pass

    # Dự phòng Ping chuẩn hệ thống
    start_t = time.perf_counter()
    flag = "-n 1 -w 800" if sys.platform.startswith("win") else "-c 1 -W 1"
    devnull = "nul" if sys.platform.startswith("win") else "/dev/null"
    success = (os.system(f"ping {flag} {target} > {devnull} 2>&1") == 0)
    elapsed_ms = round((time.perf_counter() - start_t) * 1000, 1)

    if success:
        status = "warning" if elapsed_ms > 120 else "up"
        return status, elapsed_ms
    return "down", 0.0

async def prtg_monitor_worker():
    """Vòng lặp giám sát liên tục mỗi 20 giây"""
    while True:
        db = SessionLocal()
        try:
            devices = db.query(Device).filter(Device.status != "paused").all()
            for dev in devices:
                if dev.ip_address:
                    status, ms = ping_measure(dev.ip_address)
                    dev.status = status
                    dev.latency_ms = ms
                    dev.last_checked = datetime.utcnow()
                    
                    # Lưu lại lịch sử đo độ trễ
                    db.add(PingHistory(device_id=dev.id, latency_ms=ms, status=status))

            # Giữ lại tối đa 50 bản ghi lịch sử gần nhất cho mỗi máy để tránh nặng CSDL
            db.commit()
        except Exception:
            pass
        finally:
            db.close()
        await asyncio.sleep(20)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(prtg_monitor_worker())
    asyncio.get_event_loop().call_later(1.5, lambda: webbrowser.open("http://localhost:8000"))
    yield
    task.cancel()

app = FastAPI(title="PRTG & IT Asset Sentinel", lifespan=lifespan)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ==============================================================================
# 5. RESTFUL APIS ĐẦY ĐỦ CHO HỆ THỐNG
# ==============================================================================
@app.get("/api/summary")
def get_summary(db: Session = Depends(get_db)):
    devices = db.query(Device).all()
    locations = db.query(Location).all()
    logs = db.query(MaintenanceLog).all()

    total = len(devices)
    up = sum(1 for d in devices if d.status == "up")
    warning = sum(1 for d in devices if d.status == "warning")
    down = sum(1 for d in devices if d.status == "down")
    paused = sum(1 for d in devices if d.status == "paused")
    total_val = sum(d.initial_value or 0 for d in devices)
    total_repair = sum(l.cost or 0 for l in logs)

    # Thống kê tổng hợp theo từng khu vực
    loc_stats = []
    for loc in locations:
        loc_devs = [d for d in devices if d.location_id == loc.id]
        loc_stats.append({
            "id": loc.id,
            "name": loc.name,
            "total": len(loc_devs),
            "up": sum(1 for d in loc_devs if d.status == "up"),
            "down": sum(1 for d in loc_devs if d.status == "down"),
            "value": sum(d.initial_value or 0 for d in loc_devs)
        })

    return {
        "kpi": {"total": total, "up": up, "warning": warning, "down": down, "paused": paused, "total_value": total_val, "total_repair_cost": total_repair},
        "locations": loc_stats
    }

@app.get("/api/devices")
def list_devices(db: Session = Depends(get_db)):
    devices = db.query(Device).all()
    loc_map = {l.id: l.name for l in db.query(Location).all()}
    result = []
    for d in devices:
        result.append({
            "id": d.id,
            "asset_code": d.asset_code,
            "name": d.name,
            "device_type": d.device_type,
            "ip_address": d.ip_address,
            "location_id": d.location_id,
            "location_name": loc_map.get(d.location_id, "Chưa phân nhóm"),
            "initial_value": d.initial_value,
            "status": d.status,
            "latency_ms": d.latency_ms,
            "last_checked": d.last_checked.strftime("%H:%M:%S %d/%m/%Y") if d.last_checked else ""
        })
    return result

@app.post("/api/devices")
def create_device(data: DeviceCreate, db: Session = Depends(get_db)):
    if db.query(Device).filter(Device.asset_code == data.asset_code).first():
        raise HTTPException(status_code=400, detail="Mã tài sản đã tồn tại!")
    status, ms = ping_measure(data.ip_address)
    dev = Device(**data.model_dump(), status=status, latency_ms=ms)
    db.add(dev)
    db.commit()
    db.refresh(dev)
    return dev

@app.put("/api/devices/{device_id}")
def update_device(device_id: int, data: DeviceUpdate, db: Session = Depends(get_db)):
    dev = db.query(Device).filter(Device.id == device_id).first()
    if not dev:
        raise HTTPException(status_code=404, detail="Không tìm thấy thiết bị")
    for key, value in data.model_dump().items():
        setattr(dev, key, value)
    db.commit()
    return {"message": "Cập nhật thành công"}

@app.patch("/api/devices/{device_id}/move")
def move_device(device_id: int, data: DeviceMove, db: Session = Depends(get_db)):
    dev = db.query(Device).filter(Device.id == device_id).first()
    if not dev:
        raise HTTPException(status_code=404, detail="Không tìm thấy thiết bị")
    dev.location_id = data.location_id
    db.commit()
    return {"message": "Di chuyển vị trí thành công"}

@app.patch("/api/devices/{device_id}/toggle-pause")
def toggle_pause(device_id: int, db: Session = Depends(get_db)):
    dev = db.query(Device).filter(Device.id == device_id).first()
    if not dev:
        raise HTTPException(status_code=404, detail="Không tìm thấy thiết bị")
    dev.status = "up" if dev.status == "paused" else "paused"
    db.commit()
    return {"status": dev.status}

@app.post("/api/devices/{device_id}/check-now")
def check_device_now(device_id: int, db: Session = Depends(get_db)):
    dev = db.query(Device).filter(Device.id == device_id).first()
    if not dev:
        raise HTTPException(status_code=404, detail="Không tìm thấy")
    status, ms = ping_measure(dev.ip_address)
    dev.status = status
    dev.latency_ms = ms
    dev.last_checked = datetime.utcnow()
    db.add(PingHistory(device_id=dev.id, latency_ms=ms, status=status))
    db.commit()
    return {"status": dev.status, "latency_ms": dev.latency_ms}

@app.delete("/api/devices/{device_id}")
def delete_device(device_id: int, db: Session = Depends(get_db)):
    dev = db.query(Device).filter(Device.id == device_id).first()
    if not dev:
        raise HTTPException(status_code=404, detail="Không tìm thấy")
    db.query(PingHistory).filter(PingHistory.device_id == device_id).delete()
    db.query(MaintenanceLog).filter(MaintenanceLog.device_id == device_id).delete()
    db.delete(dev)
    db.commit()
    return {"ok": True}

@app.get("/api/devices/{device_id}/details")
def get_device_details(device_id: int, db: Session = Depends(get_db)):
    dev = db.query(Device).filter(Device.id == device_id).first()
    if not dev:
        raise HTTPException(status_code=404, detail="Không tìm thấy")
    history = db.query(PingHistory).filter(PingHistory.device_id == device_id).order_by(PingHistory.recorded_at.desc()).limit(20).all()
    logs = db.query(MaintenanceLog).filter(MaintenanceLog.device_id == device_id).order_by(MaintenanceLog.maintenance_date.desc()).all()
    
    return {
        "device": dev,
        "history": [{"ms": h.latency_ms, "time": h.recorded_at.strftime("%H:%M:%S")} for h in reversed(history)],
        "logs": logs
    }

@app.get("/api/locations")
def list_locations(db: Session = Depends(get_db)):
    return db.query(Location).all()

@app.post("/api/locations")
def create_location(data: LocationCreate, db: Session = Depends(get_db)):
    loc = Location(name=data.name, description=data.description)
    db.add(loc)
    db.commit()
    db.refresh(loc)
    return loc

@app.post("/api/logs")
def create_log(item: MaintenanceCreate, db: Session = Depends(get_db)):
    log = MaintenanceLog(**item.model_dump())
    db.add(log)
    db.commit()
    return log

# ==============================================================================
# 6. GIAO DIỆN CHUYÊN NGHIỆP: PRTG MONITOR & ASSET MANAGER
# ==============================================================================
@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    return """
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>PRTG Sentinel - Network & IT Asset System</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
    </head>
    <body class="bg-[#0b101b] text-slate-100 min-h-screen font-sans selection:bg-indigo-600">
        
        <!-- Topbar PRTG Style -->
        <header class="bg-[#101726] border-b border-slate-800 px-6 py-3 flex justify-between items-center sticky top-0 z-30 shadow-md">
            <div class="flex items-center gap-4">
                <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-cyan-600 to-indigo-600 flex items-center justify-center text-white shadow-lg shadow-cyan-500/20">
                    <i class="fa-solid fa-chart-line text-lg"></i>
                </div>
                <div>
                    <h1 class="text-base font-bold text-white tracking-wide flex items-center gap-2">
                        PRTG SENTINEL <span class="text-[10px] bg-indigo-500/20 text-indigo-400 px-2 py-0.5 rounded border border-indigo-500/30">ENTERPRISE ITAM</span>
                    </h1>
                    <p class="text-xs text-slate-400">Giám sát hiệu năng mạng & Quản trị vòng đời thiết bị</p>
                </div>
            </div>

            <!-- Tab Điều Hướng Chính -->
            <div class="flex gap-1 bg-slate-900 p-1 rounded-xl border border-slate-800">
                <button onclick="switchView('view-devices')" id="tab-devices" class="px-4 py-1.5 rounded-lg text-xs font-semibold bg-indigo-600 text-white transition"><i class="fa-solid fa-network-wired mr-1"></i> Danh Sách Thiết Bị</button>
                <button onclick="switchView('view-summary')" id="tab-summary" class="px-4 py-1.5 rounded-lg text-xs font-semibold text-slate-400 hover:text-white transition"><i class="fa-solid fa-chart-pie mr-1"></i> Báo Cáo Tổng Hợp</button>
            </div>

            <div class="flex gap-2">
                <button onclick="openModal('modalLocation')" class="bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 px-3 py-2 rounded-xl text-xs font-semibold transition flex items-center gap-1.5">
                    <i class="fa-solid fa-folder-plus"></i> Tạo Khu Vực
                </button>
                <button onclick="openAddDeviceModal()" class="bg-indigo-600 hover:bg-indigo-500 text-white px-3 py-2 rounded-xl text-xs font-semibold shadow-lg shadow-indigo-600/30 transition flex items-center gap-1.5">
                    <i class="fa-solid fa-plus"></i> Thêm Thiết Bị
                </button>
            </div>
        </header>

        <main class="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">

            <!-- PRTG SENSOR STATUS BAR -->
            <div class="grid grid-cols-2 md:grid-cols-5 gap-3">
                <div class="bg-slate-900/80 border border-slate-800 p-3.5 rounded-2xl flex items-center justify-between">
                    <div>
                        <div class="text-[11px] text-slate-400">Tất cả Sensor</div>
                        <div id="kpi-total" class="text-2xl font-bold text-white mt-0.5">0</div>
                    </div>
                    <i class="fa-solid fa-server text-2xl text-slate-700"></i>
                </div>
                <div class="bg-slate-900/80 border border-slate-800 p-3.5 rounded-2xl flex items-center justify-between">
                    <div>
                        <div class="text-[11px] text-emerald-400 flex items-center gap-1.5 font-medium">
                            <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span> UP (Tốt)
                        </div>
                        <div id="kpi-up" class="text-2xl font-bold text-emerald-400 mt-0.5">0</div>
                    </div>
                    <i class="fa-solid fa-circle-check text-2xl text-emerald-950"></i>
                </div>
                <div class="bg-slate-900/80 border border-slate-800 p-3.5 rounded-2xl flex items-center justify-between">
                    <div>
                        <div class="text-[11px] text-amber-400 font-medium">WARNING (>120ms)</div>
                        <div id="kpi-warning" class="text-2xl font-bold text-amber-400 mt-0.5">0</div>
                    </div>
                    <i class="fa-solid fa-triangle-exclamation text-2xl text-amber-950"></i>
                </div>
                <div class="bg-slate-900/80 border border-slate-800 p-3.5 rounded-2xl flex items-center justify-between">
                    <div>
                        <div class="text-[11px] text-rose-400 font-medium">DOWN (Mất mạng)</div>
                        <div id="kpi-down" class="text-2xl font-bold text-rose-400 mt-0.5">0</div>
                    </div>
                    <i class="fa-solid fa-circle-xmark text-2xl text-rose-950"></i>
                </div>
                <div class="bg-slate-900/80 border border-slate-800 p-3.5 rounded-2xl flex items-center justify-between col-span-2 md:col-span-1">
                    <div>
                        <div class="text-[11px] text-slate-400 font-medium">TẠM DỪNG (Pause)</div>
                        <div id="kpi-paused" class="text-2xl font-bold text-slate-400 mt-0.5">0</div>
                    </div>
                    <i class="fa-solid fa-circle-pause text-2xl text-slate-700"></i>
                </div>
            </div>

            <!-- VIEW 1: DANH SÁCH & CÂY THIẾT BỊ (DEVICE TREE) -->
            <div id="view-devices" class="space-y-4">
                
                <!-- Thanh Lọc & Điều Hướng -->
                <div class="flex flex-col sm:flex-row justify-between items-stretch sm:items-center gap-3 bg-slate-900/60 p-3 rounded-2xl border border-slate-800">
                    <div id="location-chips" class="flex items-center gap-1.5 overflow-x-auto pb-1 sm:pb-0 scrollbar-none">
                        <!-- Nút lọc khu vực -->
                    </div>
                    <div class="flex gap-2">
                        <select id="filter-status" onchange="renderGrid()" class="bg-slate-950 border border-slate-800 rounded-xl px-3 py-1.5 text-xs text-slate-300">
                            <option value="ALL">Mọi trạng thái</option>
                            <option value="up">Trực tuyến (UP)</option>
                            <option value="warning">Cảnh báo (WARNING)</option>
                            <option value="down">Mất kết nối (DOWN)</option>
                        </select>
                        <input id="search-box" oninput="renderGrid()" placeholder="Tìm theo tên, IP, mã..." class="bg-slate-950 border border-slate-800 rounded-xl px-3 py-1.5 text-xs text-white focus:outline-none focus:border-indigo-500 w-48">
                    </div>
                </div>

                <!-- Lưới Thiết Bị -->
                <div id="device-container" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4"></div>
            </div>

            <!-- VIEW 2: BÁO CÁO TỔNG HỢP & PHÂN TÍCH TÀI CHÍNH -->
            <div id="view-summary" class="hidden space-y-6">
                
                <!-- Tổng hợp tài sản -->
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div class="bg-slate-900 border border-slate-800 p-5 rounded-2xl">
                        <span class="text-xs text-slate-400 uppercase tracking-wider font-semibold">Tổng nguyên giá tài sản toàn công ty</span>
                        <div id="sum-total-val" class="text-3xl font-bold text-amber-300 mt-2">0 đ</div>
                        <p class="text-xs text-slate-500 mt-1">Được tính từ tất cả thiết bị CNTT đang quản lý</p>
                    </div>
                    <div class="bg-slate-900 border border-slate-800 p-5 rounded-2xl">
                        <span class="text-xs text-slate-400 uppercase tracking-wider font-semibold">Tổng chi phí bảo dưỡng & sửa chữa</span>
                        <div id="sum-repair-val" class="text-3xl font-bold text-rose-400 mt-2">0 đ</div>
                        <p class="text-xs text-slate-500 mt-1">Tổng hợp từ toàn bộ lịch sử thay thế linh kiện</p>
                    </div>
                </div>

                <!-- Bảng tổng hợp chi tiết theo từng khu vực -->
                <div class="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden">
                    <div class="px-5 py-4 border-b border-slate-800 flex justify-between items-center">
                        <h3 class="text-sm font-bold text-white"><i class="fa-solid fa-layer-group text-indigo-400 mr-2"></i> Tổng Hợp Thiết Bị Theo Nhóm / Khu Vực</h3>
                    </div>
                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs text-slate-300">
                            <thead class="bg-slate-950 text-slate-400 uppercase tracking-wider text-[10px]">
                                <tr>
                                    <th class="px-5 py-3">Khu vực / Phòng ban</th>
                                    <th class="px-5 py-3 text-center">Tổng máy</th>
                                    <th class="px-5 py-3 text-center">Đang UP</th>
                                    <th class="px-5 py-3 text-center">Đang DOWN</th>
                                    <th class="px-5 py-3 text-right">Tổng giá trị</th>
                                </tr>
                            </thead>
                            <tbody id="summary-table-body" class="divide-y divide-slate-800"></tbody>
                        </table>
                    </div>
                </div>
            </div>
        </main>

        <!-- MODAL: CHI TIẾT SENSOR, ĐỒ THỊ PING PRTG & BẢO TRÌ -->
        <div id="modalDetails" class="hidden fixed inset-0 bg-black/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
            <div class="bg-slate-900 border border-slate-800 rounded-2xl max-w-3xl w-full p-6 max-h-[90vh] overflow-y-auto">
                <div class="flex justify-between items-start border-b border-slate-800 pb-4">
                    <div>
                        <div class="flex items-center gap-2">
                            <span id="det-code" class="text-xs font-mono px-2 py-0.5 rounded bg-indigo-950 text-indigo-400 font-bold"></span>
                            <span id="det-status-badge" class="text-[10px] font-bold px-2 py-0.5 rounded-full"></span>
                        </div>
                        <h2 id="det-name" class="text-lg font-bold text-white mt-1"></h2>
                    </div>
                    <button onclick="closeModal('modalDetails')" class="text-slate-400 hover:text-white"><i class="fa-solid fa-xmark text-lg"></i></button>
                </div>

                <!-- Đồ thị Sensor PRTG -->
                <div class="my-4 bg-slate-950 p-4 rounded-xl border border-slate-800">
                    <div class="flex justify-between items-center mb-2">
                        <span class="text-xs font-bold text-slate-300"><i class="fa-solid fa-chart-area text-indigo-400 mr-1.5"></i> Biến thiên độ trễ mạng (Ping Sensor ms)</span>
                        <span id="det-current-latency" class="text-xs font-mono text-cyan-400 font-bold">-- ms</span>
                    </div>
                    <div class="h-44 w-full">
                        <canvas id="pingChart"></canvas>
                    </div>
                </div>

                <!-- Thông số & QR -->
                <div class="grid grid-cols-1 sm:grid-cols-2 gap-4 bg-slate-950/60 p-4 rounded-xl border border-slate-800">
                    <div class="space-y-1.5 text-xs text-slate-300">
                        <p><strong class="text-slate-500">Phân loại:</strong> <span id="det-type"></span></p>
                        <p><strong class="text-slate-500">Địa chỉ IP:</strong> <span id="det-ip" class="font-mono text-cyan-400"></span></p>
                        <p><strong class="text-slate-500">Khu vực:</strong> <span id="det-loc"></span></p>
                        <p><strong class="text-slate-500">Nguyên giá:</strong> <span id="det-val" class="text-amber-300 font-medium"></span></p>
                    </div>
                    <div class="flex flex-col items-center justify-center bg-white p-3 rounded-lg w-fit mx-auto sm:ml-auto">
                        <div id="det-qr"></div>
                        <span class="text-[10px] text-slate-800 font-mono mt-1 font-bold">Mã QR Kiểm kê</span>
                    </div>
                </div>

                <!-- Lịch sử sửa chữa -->
                <div class="flex justify-between items-center mt-6 mb-3">
                    <h4 class="text-xs font-bold text-slate-400 uppercase tracking-wider">Lịch sử bảo trì & phụ tùng thay thế</h4>
                    <button id="btn-add-log-from-det" class="text-xs text-indigo-400 hover:underline"><i class="fa-solid fa-plus"></i> Ghi nhận bảo trì</button>
                </div>
                <div id="det-logs" class="space-y-2"></div>
            </div>
        </div>

        <!-- MODAL: THÊM / CHỈNH SỬA THIẾT BỊ (ĐẦY ĐỦ CÁC TRƯỜNG) -->
        <div id="modalDeviceForm" class="hidden fixed inset-0 bg-black/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 w-full max-w-md">
                <h3 id="dev-form-title" class="text-base font-bold text-white mb-4"></h3>
                <input type="hidden" id="form-dev-id">
                
                <div class="space-y-3 text-xs">
                    <div>
                        <label class="text-slate-400 mb-1 block">Mã tài sản (Asset Tag)</label>
                        <input id="form-code" placeholder="VD: CAM-001, PC-KT01" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white outline-none focus:border-indigo-500">
                    </div>
                    <div>
                        <label class="text-slate-400 mb-1 block">Tên thiết bị</label>
                        <input id="form-name" placeholder="VD: Camera Kho Hàng, Máy in HP" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white outline-none focus:border-indigo-500">
                    </div>
                    <div class="grid grid-cols-2 gap-2">
                        <div>
                            <label class="text-slate-400 mb-1 block">Phân loại</label>
                            <select id="form-type" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white outline-none">
                                <option value="Camera">Camera</option>
                                <option value="Máy tính">Máy tính (PC/Laptop)</option>
                                <option value="Máy in">Máy in</option>
                                <option value="Switch/Router">Switch / Router</option>
                                <option value="Máy chủ">Máy chủ (Server)</option>
                            </select>
                        </div>
                        <div>
                            <label class="text-slate-400 mb-1 block">Khu vực / Nhóm</label>
                            <select id="form-location" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white outline-none"></select>
                        </div>
                    </div>
                    <div>
                        <label class="text-slate-400 mb-1 block">Địa chỉ IP</label>
                        <input id="form-ip" placeholder="VD: 192.168.1.50" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 font-mono text-cyan-400 outline-none focus:border-indigo-500">
                    </div>
                    <div>
                        <label class="text-slate-400 mb-1 block">Nguyên giá (VNĐ)</label>
                        <input id="form-val" type="number" placeholder="VD: 12000000" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-amber-300 outline-none focus:border-indigo-500">
                    </div>
                </div>

                <div class="flex justify-end gap-2 mt-6">
                    <button onclick="closeModal('modalDeviceForm')" class="px-4 py-2 bg-slate-800 text-slate-300 rounded-xl text-xs font-medium">Hủy</button>
                    <button onclick="saveDeviceForm()" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-medium">Lưu Thiết Bị</button>
                </div>
            </div>
        </div>

        <!-- MODAL: DI CHUYỂN THIẾT BỊ SANG NHÓM KHÁC -->
        <div id="modalMove" class="hidden fixed inset-0 bg-black/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 w-full max-w-sm">
                <h3 class="text-base font-bold text-white mb-2 flex items-center gap-2">
                    <i class="fa-solid fa-arrows-turn-to-dots text-indigo-400"></i> Di Chuyển Thiết Bị
                </h3>
                <p id="move-dev-name" class="text-xs text-slate-400 mb-4"></p>
                <input type="hidden" id="move-dev-id">

                <div>
                    <label class="text-xs text-slate-400 mb-1 block">Chọn Nhóm / Khu Vực mới:</label>
                    <select id="move-target-loc" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-xs text-white outline-none"></select>
                </div>

                <div class="flex justify-end gap-2 mt-6">
                    <button onclick="closeModal('modalMove')" class="px-4 py-2 bg-slate-800 text-slate-300 rounded-xl text-xs">Hủy</button>
                    <button onclick="executeMove()" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-medium">Xác Nhận Di Chuyển</button>
                </div>
            </div>
        </div>

        <!-- MODAL: TẠO KHU VỰC MỚI -->
        <div id="modalLocation" class="hidden fixed inset-0 bg-black/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 w-full max-w-sm">
                <h3 class="text-base font-bold text-white mb-3">Tạo Nhóm / Khu Vực Mới</h3>
                <div class="space-y-3 text-xs">
                    <input id="new-loc-name" placeholder="Tên khu vực (VD: Tầng 3, Chi Nhánh 2)" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white outline-none">
                    <input id="new-loc-desc" placeholder="Mô tả thêm (Tùy chọn)" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white outline-none">
                </div>
                <div class="flex justify-end gap-2 mt-6">
                    <button onclick="closeModal('modalLocation')" class="px-4 py-2 bg-slate-800 text-slate-300 rounded-xl text-xs">Hủy</button>
                    <button onclick="createLocation()" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-medium">Tạo Khu Vực</button>
                </div>
            </div>
        </div>

        <!-- MODAL: GHI NHẬT KÝ BẢO TRÌ -->
        <div id="modalLog" class="hidden fixed inset-0 bg-black/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 w-full max-w-md">
                <h3 class="text-base font-bold text-white mb-4">Ghi nhận bảo trì & phụ tùng thay thế</h3>
                <input type="hidden" id="log-dev-id">
                <div class="space-y-3 text-xs">
                    <div>
                        <label class="text-slate-400 mb-1 block">Kỹ thuật viên thực hiện</label>
                        <input id="log-tech" placeholder="VD: Nguyễn Văn A" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white outline-none">
                    </div>
                    <div>
                        <label class="text-slate-400 mb-1 block">Linh kiện thay thế / Hành động xử lý</label>
                        <textarea id="log-parts" rows="2" placeholder="VD: Thay nguồn camera 12V-2A, nâng cấp SSD 256GB" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white outline-none"></textarea>
                    </div>
                    <div>
                        <label class="text-slate-400 mb-1 block">Chi phí sửa chữa (VNĐ)</label>
                        <input id="log-cost" type="number" placeholder="VD: 450000" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-amber-300 outline-none">
                    </div>
                </div>
                <div class="flex justify-end gap-2 mt-6">
                    <button onclick="closeModal('modalLog')" class="px-4 py-2 bg-slate-800 text-slate-300 rounded-xl text-xs">Hủy</button>
                    <button onclick="submitMaintenanceLog()" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-medium">Lưu Bảo Trì</button>
                </div>
            </div>
        </div>

        <script>
            let currentDevices = [];
            let currentLocations = [];
            let selectedLocationId = 'ALL';
            let chartInstance = null;

            async function init() {
                await fetchLocations();
                await fetchDevices();
                await fetchSummary();
                setInterval(() => {
                    fetchDevices(false);
                    fetchSummary();
                }, 15000);
            }

            async function fetchLocations() {
                const res = await fetch('/api/locations');
                currentLocations = await res.json();
                renderLocationChips();
            }

            async function fetchDevices(render = true) {
                const res = await fetch('/api/devices');
                currentDevices = await res.json();
                if (render) renderGrid();
            }

            async function fetchSummary() {
                const res = await fetch('/api/summary');
                const data = await res.json();
                
                document.getElementById('kpi-total').innerText = data.kpi.total;
                document.getElementById('kpi-up').innerText = data.kpi.up;
                document.getElementById('kpi-warning').innerText = data.kpi.warning;
                document.getElementById('kpi-down').innerText = data.kpi.down;
                document.getElementById('kpi-paused').innerText = data.kpi.paused;

                document.getElementById('sum-total-val').innerText = Number(data.kpi.total_value).toLocaleString('vi-VN') + ' đ';
                document.getElementById('sum-repair-val').innerText = Number(data.kpi.total_repair_cost).toLocaleString('vi-VN') + ' đ';

                const tbody = document.getElementById('summary-table-body');
                tbody.innerHTML = data.locations.map(l => `
                    <tr class="hover:bg-slate-800/40">
                        <td class="px-5 py-3 font-semibold text-white">${l.name}</td>
                        <td class="px-5 py-3 text-center font-bold">${l.total}</td>
                        <td class="px-5 py-3 text-center text-emerald-400 font-bold">${l.up}</td>
                        <td class="px-5 py-3 text-center text-rose-400 font-bold">${l.down}</td>
                        <td class="px-5 py-3 text-right font-medium text-amber-300">${Number(l.value).toLocaleString('vi-VN')} đ</td>
                    </tr>
                `).join('');
            }

            function renderLocationChips() {
                const container = document.getElementById('location-chips');
                let html = `<button onclick="filterByLoc('ALL')" class="px-3 py-1.5 rounded-xl text-xs font-medium ${selectedLocationId === 'ALL' ? 'bg-indigo-600 text-white' : 'bg-slate-800 text-slate-400 hover:text-white'} whitespace-nowrap">Tất cả nhóm</button>`;
                
                currentLocations.forEach(loc => {
                    const active = selectedLocationId === loc.id;
                    html += `<button onclick="filterByLoc(${loc.id})" class="px-3 py-1.5 rounded-xl text-xs font-medium ${active ? 'bg-indigo-600 text-white' : 'bg-slate-800 text-slate-400 hover:text-white'} whitespace-nowrap">${loc.name}</button>`;
                });
                container.innerHTML = html;
            }

            function filterByLoc(id) {
                selectedLocationId = id;
                renderLocationChips();
                renderGrid();
            }

            function renderGrid() {
                const container = document.getElementById('device-container');
                const search = document.getElementById('search-box').value.toLowerCase();
                const statusFilter = document.getElementById('filter-status').value;

                container.innerHTML = '';

                const filtered = currentDevices.filter(d => {
                    const matchLoc = (selectedLocationId === 'ALL') || (d.location_id === selectedLocationId);
                    const matchStatus = (statusFilter === 'ALL') || (d.status === statusFilter);
                    const matchSearch = d.name.toLowerCase().includes(search) || 
                                        d.asset_code.toLowerCase().includes(search) || 
                                        (d.ip_address && d.ip_address.includes(search));
                    return matchLoc && matchStatus && matchSearch;
                });

                if (filtered.length === 0) {
                    container.innerHTML = `<div class="col-span-full py-16 text-center text-slate-500 text-xs">Không tìm thấy thiết bị nào phù hợp tiêu chí lọc</div>`;
                    return;
                }

                filtered.forEach(d => {
                    let badgeClass = "bg-rose-500/10 text-rose-400 border border-rose-500/20";
                    let dotClass = "bg-rose-500";
                    let statusLabel = "DOWN";

                    if (d.status === "up") {
                        badgeClass = "bg-emerald-500/10 text-emerald-400 border border-emerald-500/20";
                        dotClass = "bg-emerald-400 animate-pulse";
                        statusLabel = `UP (${d.latency_ms} ms)`;
                    } else if (d.status === "warning") {
                        badgeClass = "bg-amber-500/10 text-amber-400 border border-amber-500/20";
                        dotClass = "bg-amber-400 animate-ping";
                        statusLabel = `SLOW (${d.latency_ms} ms)`;
                    } else if (d.status === "paused") {
                        badgeClass = "bg-slate-800 text-slate-400 border border-slate-700";
                        dotClass = "bg-slate-400";
                        statusLabel = "PAUSED";
                    }

                    const card = document.createElement('div');
                    card.className = "bg-slate-900 border border-slate-800 hover:border-indigo-500/50 transition rounded-2xl p-4 flex flex-col justify-between";
                    card.innerHTML = `
                        <div>
                            <div class="flex items-start justify-between">
                                <div>
                                    <span class="text-[10px] font-mono px-2 py-0.5 rounded bg-slate-950 text-indigo-400 font-bold">${d.asset_code}</span>
                                    <h3 class="text-sm font-bold text-white mt-1 cursor-pointer hover:text-indigo-400" onclick="showDeviceDetails(${d.id})">${d.name}</h3>
                                </div>
                                <span class="flex items-center gap-1.5 text-[11px] font-semibold px-2 py-0.5 rounded-full ${badgeClass}">
                                    <span class="w-1.5 h-1.5 rounded-full ${dotClass}"></span>
                                    ${statusLabel}
                                </span>
                            </div>

                            <div class="mt-4 pt-3 border-t border-slate-800/80 space-y-1.5 text-xs text-slate-400">
                                <div class="flex justify-between items-center">
                                    <span>Khu vực:</span>
                                    <span class="text-slate-200 font-medium">${d.location_name}</span>
                                </div>
                                <div class="flex justify-between items-center">
                                    <span>Địa chỉ IP:</span>
                                    <span class="font-mono text-cyan-400">${d.ip_address || 'Chưa cấu hình'}</span>
                                </div>
                                <div class="flex justify-between items-center">
                                    <span>Phân loại:</span>
                                    <span class="text-slate-300">${d.device_type}</span>
                                </div>
                                <div class="flex justify-between items-center">
                                    <span>Nguyên giá:</span>
                                    <span class="text-amber-300 font-medium">${Number(d.initial_value).toLocaleString('vi-VN')} đ</span>
                                </div>
                            </div>
                        </div>

                        <div class="mt-4 pt-3 border-t border-slate-800 flex items-center justify-between text-xs">
                            <div class="flex items-center gap-2">
                                <button onclick="checkNow(${d.id}, this)" title="Kiểm tra ngay" class="text-slate-400 hover:text-cyan-400"><i class="fa-solid fa-arrows-rotate"></i></button>
                                <button onclick="togglePause(${d.id})" title="Tạm dừng/Tiếp tục giám sát" class="text-slate-400 hover:text-amber-400"><i class="fa-solid fa-pause"></i></button>
                                <button onclick="openMoveModal(${d.id}, '${d.name}', ${d.location_id})" title="Di chuyển nhóm" class="text-slate-400 hover:text-indigo-400"><i class="fa-solid fa-arrows-turn-to-dots"></i></button>
                            </div>
                            <div class="flex items-center gap-2">
                                <button onclick="openEditDeviceModal(${d.id})" class="text-slate-400 hover:text-white" title="Sửa thông tin"><i class="fa-solid fa-pen-to-square"></i></button>
                                <button onclick="showDeviceDetails(${d.id})" class="text-indigo-400 font-medium hover:underline">Chi tiết & Log</button>
                                <button onclick="deleteDevice(${d.id})" class="text-slate-600 hover:text-rose-400"><i class="fa-solid fa-trash-can"></i></button>
                            </div>
                        </div>
                    `;
                    container.appendChild(card);
                });
            }

            async function showDeviceDetails(id) {
                const res = await fetch(`/api/devices/${id}/details`);
                const data = await res.json();
                const dev = data.device;
                const history = data.history;
                const logs = data.logs;

                document.getElementById('det-code').innerText = dev.asset_code;
                document.getElementById('det-name').innerText = dev.name;
                document.getElementById('det-type').innerText = dev.device_type;
                document.getElementById('det-ip').innerText = dev.ip_address || 'None';
                document.getElementById('det-val').innerText = Number(dev.initial_value).toLocaleString('vi-VN') + ' đ';
                document.getElementById('det-current-latency').innerText = `${dev.latency_ms} ms (${dev.status.toUpperCase()})`;

                const loc = currentLocations.find(l => l.id === dev.location_id);
                document.getElementById('det-loc').innerText = loc ? loc.name : 'Chưa phân nhóm';

                // Vẽ Biểu Đồ PRTG Sensor Latency Chart (Chart.js)
                const ctx = document.getElementById('pingChart').getContext('2d');
                if (chartInstance) chartInstance.destroy();
                
                chartInstance = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: history.map(h => h.time),
                        datasets: [{
                            label: 'Độ trễ phản hồi (ms)',
                            data: history.map(h => h.ms),
                            borderColor: '#6366f1',
                            backgroundColor: 'rgba(99, 102, 241, 0.1)',
                            borderWidth: 2,
                            fill: true,
                            tension: 0.3
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { display: false } },
                        scales: {
                            x: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#64748b', font: { size: 10 } } },
                            y: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#64748b', font: { size: 10 } }, beginAtZero: true }
                        }
                    }
                });

                // QR Code
                const qrDiv = document.getElementById('det-qr');
                qrDiv.innerHTML = '';
                new QRCode(qrDiv, { text: `ASSET:${dev.asset_code}|IP:${dev.ip_address || ''}`, width: 90, height: 90 });

                // Gắn sự kiện thêm log bảo trì
                document.getElementById('btn-add-log-from-det').onclick = () => {
                    closeModal('modalDetails');
                    document.getElementById('log-dev-id').value = dev.id;
                    openModal('modalLog');
                };

                // Danh sách Log
                const logContainer = document.getElementById('det-logs');
                if (logs.length === 0) {
                    logContainer.innerHTML = `<div class="text-xs text-slate-500 py-2 text-center">Chưa có bản ghi sửa chữa nào</div>`;
                } else {
                    logContainer.innerHTML = logs.map(l => `
                        <div class="bg-slate-950 border border-slate-800 p-3 rounded-xl text-xs space-y-1">
                            <div class="flex justify-between text-slate-400">
                                <span><i class="fa-regular fa-clock"></i> ${new Date(l.maintenance_date).toLocaleDateString('vi-VN')}</span>
                                <span class="text-slate-300 font-bold">KT: ${l.technician}</span>
                            </div>
                            <p class="text-slate-200"><strong>Nội dung / Phụ tùng:</strong> ${l.replaced_parts}</p>
                            <p class="text-amber-300 font-medium">Chi phí: ${Number(l.cost).toLocaleString('vi-VN')} đ</p>
                        </div>
                    `).join('');
                }

                openModal('modalDetails');
            }

            function openAddDeviceModal() {
                document.getElementById('dev-form-title').innerText = "Thêm Thiết Bị Mới";
                document.getElementById('form-dev-id').value = "";
                document.getElementById('form-code').value = "";
                document.getElementById('form-name').value = "";
                document.getElementById('form-ip').value = "";
                document.getElementById('form-val').value = "";
                
                populateLocationSelect('form-location');
                openModal('modalDeviceForm');
            }

            function openEditDeviceModal(id) {
                const dev = currentDevices.find(d => d.id === id);
                if (!dev) return;
                document.getElementById('dev-form-title').innerText = "Chỉnh Sửa Thiết Bị";
                document.getElementById('form-dev-id').value = dev.id;
                document.getElementById('form-code').value = dev.asset_code;
                document.getElementById('form-name').value = dev.name;
                document.getElementById('form-type').value = dev.device_type;
                document.getElementById('form-ip').value = dev.ip_address || "";
                document.getElementById('form-val').value = dev.initial_value || 0;

                populateLocationSelect('form-location', dev.location_id);
                openModal('modalDeviceForm');
            }

            async function saveDeviceForm() {
                const id = document.getElementById('form-dev-id').value;
                const payload = {
                    asset_code: document.getElementById('form-code').value.trim(),
                    name: document.getElementById('form-name').value.trim(),
                    device_type: document.getElementById('form-type').value,
                    ip_address: document.getElementById('form-ip').value.trim(),
                    location_id: parseInt(document.getElementById('form-location').value),
                    initial_value: parseFloat(document.getElementById('form-val').value) || 0
                };

                if (!payload.asset_code || !payload.name) {
                    alert("Vui lòng điền đủ Mã tài sản và Tên thiết bị!");
                    return;
                }

                const url = id ? `/api/devices/${id}` : '/api/devices';
                const method = id ? 'PUT' : 'POST';

                const res = await fetch(url, {
                    method: method,
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(payload)
                });

                if (!res.ok) {
                    const err = await res.json();
                    alert(err.detail || "Có lỗi xảy ra");
                    return;
                }

                closeModal('modalDeviceForm');
                await fetchDevices();
                await fetchSummary();
            }

            function openMoveModal(id, name, currentLocId) {
                document.getElementById('move-dev-id').value = id;
                document.getElementById('move-dev-name').innerText = `Thiết bị: ${name}`;
                populateLocationSelect('move-target-loc', currentLocId);
                openModal('modalMove');
            }

            async function executeMove() {
                const id = document.getElementById('move-dev-id').value;
                const targetLocId = parseInt(document.getElementById('move-target-loc').value);

                await fetch(`/api/devices/${id}/move`, {
                    method: 'PATCH',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ location_id: targetLocId })
                });

                closeModal('modalMove');
                await fetchDevices();
                await fetchSummary();
            }

            async function togglePause(id) {
                await fetch(`/api/devices/${id}/toggle-pause`, { method: 'PATCH' });
                await fetchDevices();
                await fetchSummary();
            }

            async function checkNow(id, btn) {
                const icon = btn.querySelector('i');
                icon.classList.add('fa-spin');
                try {
                    await fetch(`/api/devices/${id}/check-now`, { method: 'POST' });
                    await fetchDevices();
                    await fetchSummary();
                } finally {
                    icon.classList.remove('fa-spin');
                }
            }

            async function deleteDevice(id) {
                if (!confirm("Bạn có chắc chắn muốn xóa thiết bị này khỏi hệ thống?")) return;
                await fetch(`/api/devices/${id}`, { method: 'DELETE' });
                await fetchDevices();
                await fetchSummary();
            }

            async function createLocation() {
                const name = document.getElementById('new-loc-name').value.trim();
                const desc = document.getElementById('new-loc-desc').value.trim();
                if (!name) return alert("Vui lòng nhập tên khu vực!");

                await fetch('/api/locations', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ name: name, description: desc })
                });

                closeModal('modalLocation');
                document.getElementById('new-loc-name').value = '';
                await fetchLocations();
                await fetchSummary();
            }

            async function submitMaintenanceLog() {
                const payload = {
                    device_id: parseInt(document.getElementById('log-dev-id').value),
                    technician: document.getElementById('log-tech').value.trim() || 'IT Support',
                    replaced_parts: document.getElementById('log-parts').value.trim(),
                    cost: parseFloat(document.getElementById('log-cost').value) || 0
                };
                await fetch('/api/logs', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(payload)
                });
                closeModal('modalLog');
                alert("Đã ghi nhận nhật ký bảo dưỡng thành công!");
                await fetchSummary();
            }

            function populateLocationSelect(elementId, selectedId = null) {
                const sel = document.getElementById(elementId);
                sel.innerHTML = currentLocations.map(l => `
                    <option value="${l.id}" ${selectedId === l.id ? 'selected' : ''}>${l.name}</option>
                `).join('');
            }

            function switchView(viewId) {
                document.getElementById('view-devices').classList.add('hidden');
                document.getElementById('view-summary').classList.add('hidden');
                document.getElementById(viewId).classList.remove('hidden');

                if (viewId === 'view-devices') {
                    document.getElementById('tab-devices').className = "px-4 py-1.5 rounded-lg text-xs font-semibold bg-indigo-600 text-white transition";
                    document.getElementById('tab-summary').className = "px-4 py-1.5 rounded-lg text-xs font-semibold text-slate-400 hover:text-white transition";
                } else {
                    document.getElementById('tab-summary').className = "px-4 py-1.5 rounded-lg text-xs font-semibold bg-indigo-600 text-white transition";
                    document.getElementById('tab-devices').className = "px-4 py-1.5 rounded-lg text-xs font-semibold text-slate-400 hover:text-white transition";
                }
            }

            function openModal(id) { document.getElementById(id).classList.remove('hidden'); }
            function closeModal(id) { document.getElementById(id).classList.add('hidden'); }

            window.onload = init;
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_config=None)
