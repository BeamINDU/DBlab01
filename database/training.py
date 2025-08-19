import logging
from fastapi import WebSocket, WebSocketDisconnect
from database.connect_to_db import Session, text
from typing import Dict, List
import asyncio
import os
import shutil
import random
import json
from fastapi.encoders import jsonable_encoder
import subprocess
import yaml  
import sys
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from queue import Queue
import math
import json
from sqlalchemy import text
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('training.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
active_websockets: Dict[str, List[WebSocket]] = {}
pending_updates: Dict[str, asyncio.Queue] = {}
executor = ThreadPoolExecutor(max_workers=1)

# Global dictionary to track active training processes by model version ID
active_training_processes: Dict[str, subprocess.Popen] = {}

# Global list to store PID information in JSON format
training_pids: List[Dict] = []

def add_training_pid(modelversionid: int, pid: int):
    """Add PID information to the global tracking list"""
    global training_pids
    
    # ตรวจสอบว่า training_pids เป็น list
    if not isinstance(training_pids, list):
        logging.error(f"[PID_TRACKING] training_pids is not a list, resetting to empty list. Current type: {type(training_pids)}")
        training_pids = []
    
    pid_info = {
        "modelversionid": str(modelversionid),
        "PID": pid
    }
    training_pids.append(pid_info)
    logging.info(f"[PID_TRACKING] Added PID info: {pid_info}")

def remove_training_pid(modelversionid: int, pid: int):
    """Remove PID information from the global tracking list"""
    global training_pids
    
    # ตรวจสอบว่า training_pids เป็น list
    if not isinstance(training_pids, list):
        logging.error(f"[PID_TRACKING] training_pids is not a list, resetting to empty list. Current type: {type(training_pids)}")
        training_pids = []
        return
    
    # ตรวจสอบแต่ละ item ก่อนใช้ .get()
    new_training_pids = []
    for p in training_pids:
        if isinstance(p, dict):
            if not (p.get("modelversionid") == str(modelversionid) and p.get("PID") == pid):
                new_training_pids.append(p)
        else:
            logging.warning(f"[PID_TRACKING] Invalid PID item format in remove: {p}")
    
    training_pids = new_training_pids
    logging.info(f"[PID_TRACKING] Removed PID info for model version {modelversionid}, PID {pid}")

def get_pids_by_modelversion(modelversionid: int):
    """Get all PIDs for a specific model version"""
    global training_pids
    
    # ตรวจสอบว่า training_pids เป็น list
    if not isinstance(training_pids, list):
        logging.error(f"[PID_TRACKING] training_pids is not a list, resetting to empty list. Current type: {type(training_pids)}")
        training_pids = []
        return []
    
    # ตรวจสอบแต่ละ item ก่อนใช้ .get()
    result = []
    for p in training_pids:
        if isinstance(p, dict) and p.get("modelversionid") == str(modelversionid):
            result.append(p)
        elif not isinstance(p, dict):
            logging.warning(f"[PID_TRACKING] Invalid PID item format in get: {p}")
    
    return result

def load_training_pids():
    """Initialize PID tracking list"""
    global training_pids
    training_pids = []
    logging.info(f"[PID_TRACKING] Initialized empty PID tracking list")

# Initialize PID tracking on module import
load_training_pids()

def reset_pid_tracking():
    """Reset PID tracking to clean state"""
    global training_pids
    training_pids = []
    logging.info(f"[PID_TRACKING] PID tracking reset successfully")

def debug_pid_status():
    """Debug function to check PID tracking status"""
    global training_pids
    
    status_info = {
        "pid_list_type": type(training_pids).__name__,
        "pid_count": len(training_pids) if isinstance(training_pids, (list, dict)) else "N/A",
        "pid_data": training_pids
    }
    
    logging.info(f"[PID_DEBUG] Status: {status_info}")
    return status_info

def normalize(x_center, y_center, width, height , IMG_WIDTH , IMG_HEIGHT):
    return x_center / IMG_WIDTH, y_center / IMG_HEIGHT, width / IMG_WIDTH, height / IMG_HEIGHT

def normalize_point(x, y, img_w, img_h):
    return x / img_w, y / img_h

def circle_to_polygon(center, radius, num_points=16):
    cx, cy = center
    return [
        (
            cx + radius * math.cos(2 * math.pi * i / num_points),
            cy + radius * math.sin(2 * math.pi * i / num_points)
        )
        for i in range(num_points)
    ]

def rectangle_to_polygon(x1, y1, x2, y2):
    return [(x1, y1), (x2, y1), (x2, y2), (x1, y2)]

def calculate_split_sizes(total_images: int, train_percent: float, val_percent: float, test_percent: float):
    """
    คำนวณขนาดของแต่ละ set อย่าง dynamic รับประกันว่าทุก set จะมีรูปอย่างน้อย 1 รูป
    """
    # ถ้ามีรูปน้อยมาก ให้มีการจัดการพิเศษ
    if total_images <= 3:
        # สำหรับรูปน้อยมาก ให้แต่ละ set มีรูปอย่างน้อย 1 รูป
        min_size = 1
        return {
            'train': min(total_images, max(min_size, total_images - 2)) if total_images > 2 else 1,
            'val': min_size if total_images > 1 else 1,
            'test': min_size
        }
    
    # คำนวณขนาดตามเปอร์เซ็นต์
    train_size = max(1, round(total_images * train_percent / 100))
    val_size = max(1, round(total_images * val_percent / 100))
    test_size = max(1, round(total_images * test_percent / 100))
    
    # ปรับขนาดให้ไม่เกินจำนวนรูปทั้งหมด
    total_calculated = train_size + val_size + test_size
    
    if total_calculated > total_images:
        # ถ้าเกิน ให้ปรับลดตามลำดับความสำคัญ (train > val > test)
        excess = total_calculated - total_images
        
        # ลด test ก่อน (แต่ไม่ให้น้อยกว่า 1)
        reduce_test = min(excess, test_size - 1)
        test_size -= reduce_test
        excess -= reduce_test
        
        # ถ้ายังเกิน ลด val (แต่ไม่ให้น้อยกว่า 1)
        if excess > 0:
            reduce_val = min(excess, val_size - 1)
            val_size -= reduce_val
            excess -= reduce_val
            
        # ถ้ายังเกิน ลด train (แต่ไม่ให้น้อยกว่า 1)
        if excess > 0:
            train_size -= excess
            train_size = max(1, train_size)
    
    elif total_calculated < total_images:
        # ถ้าน้อยกว่า ให้เพิ่มใน train
        train_size += total_images - total_calculated
    
    return {
        'train': train_size,
        'val': val_size,
        'test': test_size
    }

def create_splits(images: list, sizes: dict):
    """
    สร้าง splits จากรูปและขนาดที่กำหนด รองรับการใช้รูปซ้ำเมื่อจำเป็น
    """
    total_images = len(images)
    train_size = sizes['train']
    val_size = sizes['val']
    test_size = sizes['test']
    
    splits = {}
    
    if total_images == 1:
        # รูปเดียว ใช้ทั้ง 3 set
        splits = {
            "train": images * min(train_size, 1),
            "val": images * min(val_size, 1), 
            "test": images * min(test_size, 1)
        }
    elif total_images == 2:
        # 2 รูป จัดสรรอย่างชาญฉลาด
        splits = {
            "train": images[:min(train_size, 1)],
            "val": images[1:2] if val_size > 0 else [],
            "test": images[:min(test_size, 1)]  # ใช้รูปแรกซ้ำถ้าจำเป็น
        }
    else:
        # มากกว่า 2 รูป แบ่งตามปกติ
        current_index = 0
        
        # Train set
        train_end = min(current_index + train_size, total_images)
        splits["train"] = images[current_index:train_end]
        current_index = train_end
        
        # Val set
        if current_index < total_images:
            val_end = min(current_index + val_size, total_images)
            splits["val"] = images[current_index:val_end]
            current_index = val_end
        else:
            # ถ้ารูปไม่พอ ใช้จากต้น
            splits["val"] = images[:min(val_size, total_images)]
        
        # Test set
        if current_index < total_images:
            splits["test"] = images[current_index:current_index + test_size]
        else:
            # ถ้ารูปไม่พอ ใช้จากต้น
            splits["test"] = images[:min(test_size, total_images)]
    
    return splits

def split_and_copy_images(model_data: dict):
    model_id = model_data.get("modelversionid", "unknown")
    logging.info(f"[SPLIT_COPY] Starting split and copy images for model version ID: {model_id}")
    
    base_dataset_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dataset"))
    images = model_data.get("images", [])

    if not images:
        logging.error(f"[SPLIT_COPY] No images found in model_data for model ID: {model_id}")
        print("dont found image model_data")
        return

    logging.info(f"[SPLIT_COPY] Found {len(images)} images for model ID: {model_id}")
    
    random.shuffle(images)
    total = len(images)

    train_percent = model_data.get("trainpercent", model_data['trainpercent'])
    val_percent = model_data.get("valpercent", model_data['valpercent'])
    test_percent = model_data.get("testpercent", model_data['testpercent'])

    logging.info(f"[SPLIT_COPY] Split percentages for model ID {model_id}: Train={train_percent}%, Val={val_percent}%, Test={test_percent}%")

    # คำนวณขนาดแต่ละ set อย่าง dynamic
    sizes = calculate_split_sizes(total, train_percent, val_percent, test_percent)
    
    # สร้าง splits
    splits = create_splits(images, sizes)

    logging.info(f"[SPLIT_COPY] Split sizes for model ID {model_id} - Total: {total}, Train: {sizes['train']}, Val: {sizes['val']}, Test: {sizes['test']}")
    logging.info(f"[SPLIT_COPY] Actual splits for model ID {model_id} - Train: {len(splits['train'])}, Val: {len(splits['val'])}, Test: {len(splits['test'])}")

    print(f" Total: {total}, Train: {sizes['train']}, Val: {sizes['val']}, Test: {sizes['test']}")
    print(f" Actual splits - Train: {len(splits['train'])}, Val: {len(splits['val'])}, Test: {len(splits['test'])}")

    class_names_set = set()

    first_image_path = images[0]["imagepath"]
    dataset_dir = os.path.dirname(os.path.abspath(os.path.join(base_dataset_path, first_image_path)))
    
    logging.info(f"[SPLIT_COPY] Dataset directory for model ID {model_id}: {dataset_dir}")

    for split_name in ["train", "val", "test"]:
        image_list = splits.get(split_name, [])
        image_dest_dir = os.path.join(dataset_dir, split_name, "images")
        label_dest_dir = os.path.join(dataset_dir, split_name, "labels")
        os.makedirs(image_dest_dir, exist_ok=True)
        os.makedirs(label_dest_dir, exist_ok=True)

        for image in image_list:
            image_width = image['width']
            image_height = image['height']
            image_rel_path = os.path.normpath(image["imagepath"].replace('\\', '/'))
            filename = os.path.basename(image_rel_path)
            name_without_ext = os.path.splitext(filename)[0]

            original_path = os.path.join(base_dataset_path, image_rel_path)
            dest_image_path = os.path.join(image_dest_dir, filename)

            try:
                shutil.copyfile(original_path, dest_image_path)
            except FileNotFoundError:
                print(f"[NOT FOUND] {original_path}")
                continue

            label_file_path = os.path.join(label_dest_dir, f"{name_without_ext}.txt")
            try:
                with open(label_file_path, "w", encoding="utf-8") as label_file:
                    for ann in image.get("annotate", []):
                        ann_type = ann.get("type")
                        class_name = ann.get("class", {}).get("name", "unknown")
                        class_names_set.add(class_name)
                        class_id = list(class_names_set).index(class_name)
                        print(f"sample {class_name}")
                        # Convert all types to polygon
                        if ann_type == "polygon":
                            points = ann.get("points", [])

                        elif ann_type == "circle":
                            center = ann.get("center", [0, 0])
                            radius = ann.get("radius", 0)
                            points = circle_to_polygon(center, radius)

                        elif ann_type == "rectangle":
                            x1, y1, x2, y2 = ann.get("bbox", [0, 0, 0, 0])
                            points = rectangle_to_polygon(x1, y1, x2, y2)

                        else:
                            print(f"[SKIP] Unsupported type: {ann_type}")
                            continue

                        if len(points) < 3:
                            print(f"[WARN] Invalid polygon, skipping")
                            continue

                        # Bounding box
                        xs, ys = zip(*points)
                        x_min, x_max = min(xs), max(xs)
                        y_min, y_max = min(ys), max(ys)
                        x_center = (x_min + x_max) / 2
                        y_center = (y_min + y_max) / 2
                        width = x_max - x_min
                        height = y_max - y_min

                        # Normalize box
                        x, y, w, h = normalize(x_center, y_center, width, height, image_width, image_height)

                        # Normalize points
                        norm_points = []
                        for px, py in points:
                            nx, ny = normalize_point(px, py, image_width, image_height)
                            norm_points.extend([f"{nx:.6f}", f"{ny:.6f}"])

                        # Write label line
                        label_line = f"{class_id} {x:.6f} {y:.6f} {w:.6f} {h:.6f} " + " ".join(norm_points)
                        label_file.write(label_line + "\n")

                print(f"[OK] Wrote seg-label: {label_file_path}")
            except Exception as e:
                print(f"[ERROR] Writing label: {e}")

    class_names = sorted(class_names_set)
    yaml_data = {
        "train": os.path.join(dataset_dir, "train", "images").replace("\\", "/"),
        "val": os.path.join(dataset_dir, "val", "images").replace("\\", "/"),
        "test": os.path.join(dataset_dir, "test", "images").replace("\\", "/"),
        "nc": len(class_names),
        "names": class_names,
        "obb": True,  # Assuming OBB is always true for this dataset
    }

    yaml_path = os.path.join(dataset_dir, "data.yaml")
    try:
        with open(yaml_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f, default_flow_style=None, sort_keys=False, allow_unicode=True)
        print(f"Created dataset yaml at: {yaml_path}")
    except Exception as e:
        print(f"[ERROR] writing YAML file: {e}")

def run_training_subprocess(model_script_path,path_output, subpath_output,type_model, weights_path, dataset_yaml, nameFolder, idFunction, epochs, log_queue: Queue, modelversionid: int = None):
    process = None
    process_key = f"{modelversionid}_{idFunction}" if modelversionid and idFunction else nameFolder
    
    try:
        # สร้าง command list
        command = [
            "python", model_script_path,
            "--weights", weights_path,
            "--data",    dataset_yaml,
            "--epochs", str(epochs),
            "--project", "runs/train",
            "--name", f"{nameFolder}",
            "--path_output", path_output,
            "--idFunction", str(idFunction),
            "--subpath_output", subpath_output,
            "--type_model", type_model
        ]
        
        # Log command ที่จะรัน
        command_str = " ".join(command)
        logging.info(f"[SUBPROCESS] Starting training subprocess for {nameFolder}")
        logging.info(f"[SUBPROCESS] Command: {command_str}")
        log_queue.put(f"[SUBPROCESS] Starting training subprocess for {nameFolder}\n")
        log_queue.put(f"[SUBPROCESS] Command: {command_str}\n")
        
        # เริ่ม subprocess
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )
        
        # Store process in global tracking dictionary
        global active_training_processes
        active_training_processes[process_key] = process

        # Add PID tracking information
        if modelversionid:
            add_training_pid(modelversionid, process.pid)
     
        logging.info(f"[SUBPROCESS] Process started with PID: {process.pid}")
        log_queue.put(f"[SUBPROCESS] Process started with PID: {process.pid}\n")

        # อ่าน output with timeout protection
        try:
            for line in process.stdout:
                log_queue.put(line)

            # รอให้ process เสร็จสิ้นด้วย timeout
            process.wait(timeout=3600)  # 1 hour timeout
            
        except subprocess.TimeoutExpired:
            logging.error(f"[SUBPROCESS] Training timeout for {nameFolder}")
            log_queue.put(f"[SUBPROCESS] Training timeout for {nameFolder}\n")
            process.kill()
            process.wait()
            return -2
        
        # Log ผลลัพธ์
        logging.info(f"[SUBPROCESS] Process finished with return code: {process.returncode}")
        log_queue.put(f"[SUBPROCESS] Process finished with return code: {process.returncode}\n")
        
        # Remove process from global tracking dictionary
        if process_key in active_training_processes:
            del active_training_processes[process_key]
            logging.info(f"[SUBPROCESS] Removed process {process_key} from active tracking")
        
        # Remove PID tracking information
        if modelversionid and process:
            remove_training_pid(modelversionid, process.pid)
        
        if process.returncode == 0:
            logging.info(f"[SUBPROCESS] Training completed successfully for {nameFolder}")
            log_queue.put(f"[SUBPROCESS] Training completed successfully for {nameFolder}\n")
        else:
            logging.error(f"[SUBPROCESS] Training failed for {nameFolder} with return code: {process.returncode}")
            log_queue.put(f"[SUBPROCESS] Training failed for {nameFolder} with return code: {process.returncode}\n")
        
        return process.returncode
        
    except Exception as e:
        error_msg = f"[SUBPROCESS ERROR] Exception in run_training_subprocess: {str(e)}"
        logging.error(error_msg)
        log_queue.put(f"{error_msg}\n")
        
        # Cleanup process if it exists
        if process:
            try:
                logging.info(f"[SUBPROCESS] Cleaning up process for {nameFolder}")
                process.kill()
                process.wait(timeout=5)
            except Exception as cleanup_error:
                logging.error(f"[SUBPROCESS] Error during cleanup: {str(cleanup_error)}")
        
        # Remove process from global tracking dictionary on error
        if process_key in active_training_processes:
            del active_training_processes[process_key]
            logging.info(f"[SUBPROCESS] Removed process {process_key} from active tracking due to error")
        
        # Remove PID tracking information on error
        if modelversionid and process:
            remove_training_pid(modelversionid, process.pid)
        
        return -1

async def start_training(dataset: dict,nameFolder: str, db: Session, websocket: WebSocket , model_name: str , idFunction: int = None):
    # images = dataset.get("images", [])
    # await websocket.send_json({"status": "mockup", "message": "Training mockup started" , "data" : images[0]["imagepath"]})
    # await asyncio.sleep(10)
    # await websocket.send_json({"status": "mockup", "message": "Training mockup in progress..."})
    # await asyncio.sleep(15)
    # await websocket.send_json({"status": "mockup", "message": "Training mockup complete"})
    
    logging.info(f"[TRAINING] Starting training for model: {nameFolder} with function ID: {idFunction}")
    
    images = dataset.get("images", [])
    # print(f"{    dataset['epochs']}")
    if not images:
        logging.error(f"[TRAINING] No images in dataset for model: {nameFolder}")
        await websocket.send_json({"status": "error", "message": "No images in dataset"})
        return

    base_dir = os.getcwd()
    first_image_path = images[0]["imagepath"]
    base_dataset_path = os.path.join(base_dir, "dataset")
    dataset_dir = os.path.dirname(os.path.abspath(os.path.join(base_dataset_path, first_image_path)))
    model_script_path = os.path.join(base_dir, "models", "train.py")
    weights_path = os.path.join(base_dir, "models", model_name)
    dataset_yaml = os.path.join(dataset_dir, "data.yaml")

    logging.info(f"[TRAINING] Training paths for {nameFolder}:")
    logging.info(f"  - Model script: {model_script_path}")
    logging.info(f"  - Weights: {weights_path}")
    logging.info(f"  - Dataset YAML: {dataset_yaml}")
    logging.info(f"  - Dataset directory: {dataset_dir}")

    await websocket.send_json({
        "status": "paths",
        "model_script_path": model_script_path,
        "weights_path": weights_path,
        "dataset_yaml": dataset_yaml,
        "dataset_dir": dataset_dir,
        "base_dataset_path": base_dataset_path
    })

    for path, label in [(model_script_path, "Training script"),
                        (weights_path, "Weights"),
                        (dataset_yaml, "Dataset config")]:
        if not os.path.exists(path):
            logging.error(f"[TRAINING] {label} not found at {path} for model: {nameFolder}")
            await websocket.send_json({"status": "error", "message": f"{label} not found at {path}"})
            return

    logging.info(f"[TRAINING] All required files exist for model: {nameFolder}")
    
    log_queue = Queue()
    print(model_name)
    parts = first_image_path.split("/")
    path_output = parts[0]
    subpath_output =  parts[1]
    type_model = model_name.replace(".pt", "")
    
    logging.info(f"[TRAINING] Training parameters for {nameFolder}:")
    logging.info(f"  - Epochs: {dataset['epochs']}")
    logging.info(f"  - Model type: {type_model}")
    logging.info(f"  - Path output: {path_output}")
    logging.info(f"  - Subpath output: {subpath_output}")
    
    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(
        executor,
        run_training_subprocess,
        model_script_path,
        path_output, 
        subpath_output,
        type_model , 
        weights_path,
        dataset_yaml,
        nameFolder,
        idFunction,
        dataset['epochs'],
        log_queue,
        dataset.get('modelversionid')  # Add model version ID
    )

    try:
        while True:
            await asyncio.sleep(0.5)

            while not log_queue.empty():
                log_line = log_queue.get()
                log_line_stripped = log_line.strip()
                # Always send log lines to the websocket for debugging
                await websocket.send_json({
                    "status": "training_log",
                    "log": log_line_stripped
                })
                # Additionally, if the line matches the epoch pattern, send epoch info
                match = re.match(r"(\d+)/(\d+)\s", log_line_stripped)
                if match:
                    epoch = match.group(1)
                    await websocket.send_json({
                        "status": "training_progress",
                        "log": log_line_stripped,
                        "epoch": int(epoch)
                    })

            if future.done():
                break

        returncode = future.result()

        logging.info(f"[TRAINING] Training subprocess completed for {nameFolder} with return code: {returncode}")

        if returncode == 0:
            # ใช้ local variable แทน global เพื่อหลีกเลี่ยง race condition
            model_path = f"detection_app/models/{path_output}/{subpath_output}/{idFunction}/best.pt"
            
            logging.info(f"[TRAINING] Training successful for {nameFolder}. Model path: {model_path}")
            
            try:
                # อ่านค่า modelpath เดิมจาก database ก่อน
                existing_sql = text("""
                    SELECT modelpath 
                    FROM modelversion 
                    WHERE modelversionid = :modelversionid
                """)
                existing_result = db.execute(existing_sql, {"modelversionid": dataset['modelversionid']}).fetchone()
                
                # Parse existing modelpath หรือสร้างใหม่ถ้าไม่มี
                if existing_result and existing_result[0]:
                    try:
                        existing_modelpath = json.loads(existing_result[0])
                        if not isinstance(existing_modelpath, dict):
                            existing_modelpath = {}
                    except (json.JSONDecodeError, TypeError):
                        existing_modelpath = {}
                else:
                    existing_modelpath = {}
                
                # เพิ่ม model path ใหม่เข้าไปใน dict เดิม
                existing_modelpath[nameFolder] = model_path
                
                logging.info(f"[TRAINING] Updated model paths: {existing_modelpath}")
                
                # ตรวจสอบว่าทุก functions ใน modelversion นี้เทรนเสร็จหมดแล้วหรือยัง
                check_functions_sql = text("""
                    SELECT COUNT(mf.functionid) as total_functions
                    FROM modelfunction mf
                    WHERE mf.modelversionid = :modelversionid
                """)
                functions_result = db.execute(check_functions_sql, {"modelversionid": dataset['modelversionid']}).fetchone()
                total_functions = functions_result[0] if functions_result else 0
                
                # ตรวจสอบจำนวน model paths ที่มีใน JSON
                completed_functions = len(existing_modelpath)
                
                logging.info(f"[TRAINING] Training progress for modelversion {dataset['modelversionid']}: {completed_functions}/{total_functions} functions completed")
                
                # กำหนด status ตามความคืบหน้า
                if completed_functions >= total_functions and total_functions > 0:
                    model_status = 'Ready'
                    logging.info(f"[TRAINING] All functions completed for modelversion {dataset['modelversionid']}, setting status to 'Ready'")
                else:
                    model_status = 'Training'
                    logging.info(f"[TRAINING] Training still in progress for modelversion {dataset['modelversionid']}, keeping status as 'Training'")
                
                # Database transaction with error handling
                sql = text("""
                    UPDATE modelversion 
                    SET modelstatus = :status,
                        currentstep = :currentstep,
                        modelpath = :modelpath
                    WHERE modelversionid = :modelversionid
                """)

                result = db.execute(sql, {
                    "status": model_status, 
                    "currentstep": 4 if model_status == "Ready" else 3,  # Increment current step
                    "modelpath": json.dumps(existing_modelpath), 
                    "modelversionid": dataset['modelversionid']
                })
                
                if result.rowcount == 0:
                    logging.warning(f"[TRAINING] No rows updated for model version ID: {dataset['modelversionid']}")
                    await websocket.send_json({"status": "warning", "message": f"No model version found to update for ID: {dataset['modelversionid']}"})
                
                db.commit()
                logging.info(f"[TRAINING] Database updated for model version ID: {dataset['modelversionid']}")
                
                # Send success messages only after successful database update
                await websocket.send_json({"status": "training complete", "message": f"Model {nameFolder} trained successfully"})
                await websocket.send_json({"status": "model_path", "model_path": f"{dataset['modelversionid']}"})
                
            except Exception as db_error:
                logging.error(f"[TRAINING] Database update failed for {nameFolder}: {str(db_error)}")
                try:
                    db.rollback()
                except Exception as rollback_error:
                    logging.error(f"[TRAINING] Rollback failed: {str(rollback_error)}")
                await websocket.send_json({"status": "error", "message": f"Database update failed: {str(db_error)}"})
                
        else:
            logging.error(f"[TRAINING] Training failed for {nameFolder} with return code: {returncode}")
            await websocket.send_json({"status": "error", "message": f"Training failed with code {returncode}"})

    except Exception as e:
        logging.exception(f"[TRAINING] Exception in start_training for {nameFolder}: {e}")
        await websocket.send_json({"status": "error", "message": f"Training failed: {str(e)}"})

async def send_keepalive(websocket: WebSocket, model_id: str, stop_event: asyncio.Event):
    try:
        while not stop_event.is_set():
            await websocket.send_json({"status": "training", "message": "still training..."})
            await asyncio.sleep(5)  
    except Exception as e:
        logging.warning(f"[KEEPALIVE ERROR] model_id={model_id}: {e}")

async def training_action_ws_handler(websocket: WebSocket, model_id: str, db: Session):
    keepalive_task = None
    stop_event = asyncio.Event()
    
    try:
        await websocket.accept()
        
        logging.info(f"[WS_HANDLER] WebSocket connection accepted for model ID: {model_id}")
        
        if model_id not in active_websockets:
            active_websockets[model_id] = []
        active_websockets[model_id].append(websocket)

        if model_id not in pending_updates:
            pending_updates[model_id] = asyncio.Queue()
        
        keepalive_task = asyncio.create_task(send_keepalive(websocket, model_id, stop_event))
    
        logging.info(f"[WS_HANDLER] Querying database for model version ID: {model_id}")
        
        sql = text("""
             SELECT
                mv.modelversionid,
                mv.modelname,
                mv.modelid,
                mv.modelstatus,
                mv.modelpath,
                mv.trainpercent,
                mv.testpercent,
                mv.valpercent,
                mv.epochs,
                mv.currentstep,
                f.functionid , 
                f.functionname,
                jsonb_agg(
                    jsonb_build_object(
                        'imageid', img.imageid,
                        'imagename', img.imagename,
                        'imagepath', img.imagepath,
                        'splitpath', img.splitpath,
                        'annotate', img.annotate ,
                        'width' , img.width , 
                        'height' , img.height
                    )
                ) AS images
            FROM modelversion mv
            LEFT JOIN model m ON mv.modelid = m.modelid
            LEFT JOIN image img ON img.modelversionid = mv.modelversionid
                AND jsonb_typeof(img.annotate::jsonb) = 'array'
                AND jsonb_array_length(img.annotate::jsonb) > 0
            left join modelfunction mf on mv.modelversionid = mf.modelversionid
            left join "function" f  on mf.functionid = f.functionid
            WHERE mv.modelversionid = :modelversionid
            GROUP BY
                mv.modelversionid,
                mv.modelname,
                mv.modelid,
                mv.modelstatus,
                mv.modelpath,
                mv.trainpercent,
                mv.testpercent,
                mv.valpercent,
                mv.epochs,
                f.functionid , 
                f.functionname,
                mv.currentstep;
        """)

        row = db.execute(sql, {"modelversionid": int(model_id)}).mappings().fetchall()
        if not row:
            logging.error(f"[WS_HANDLER] No model version found for ID: {model_id}")
            await websocket.send_json({"error": "No model version found"})
            return
            
        logging.info(f"[WS_HANDLER] Found {len(row)} model(s) for version ID: {model_id}")
        sql_update_modelStatus = text("""
            UPDATE modelversion
            SET modelstatus = 'Training',
                currentstep = 3
                                      
            WHERE modelversionid = :modelversionid
        """)
        db.execute(sql_update_modelStatus, {"modelversionid": int(model_id)})
        db.commit()
        logging.info(f"[WS_HANDLER] Model version ID {model_id} status updated to 'Processing'")
        
        if len(row) == 1 :
            # await websocket.send_json({"data": row[0]})
            row = row[0]
            logging.info(f"[WS_HANDLER] Processing single model: {row.get('modelname')} (ID: {row.get('modelversionid')})")
            
            split_and_copy_images(row)
            print(row)
            print(f"split and copy images {row.get('modelversionid')}")
            logging.info(f"[WS_HANDLER] Dataset prepared for model version ID: {row.get('modelversionid')}")
            await websocket.send_json({"status": "dataset prepared"})
            
            type_model = "yolo11n-seg.pt"
            if row.get("functionname") == "yolov8n-seg.pt":
                type_model = "yolov8n-seg.pt"
            elif row.get("functionname") == "yolov8m.pt":
                type_model = "yolov8m.pt"
            elif row.get("functionname") == "yolov8l.pt":
                type_model = "yolov8l.pt"
            elif row.get("functionname") == "Color Check":
                type_model = "box-detector_seg.pt"
                
            logging.info(f"[WS_HANDLER] Selected model type: {type_model} for function: {row.get('functionname')}")
            
            await websocket.send_json({"status": "training started"})
            nameFolder = f"model_{row.get('modelversionid')}_{row.get('functionid')}"
            function_id = row.get('functionid')
            
            logging.info(f"[WS_HANDLER] Starting training for {nameFolder}")
            await  start_training(row,nameFolder, db ,websocket, type_model,  idFunction=function_id)
            # await websocket.send_json({"data": row})
            stop_event.set()
            await keepalive_task  

            logging.info(f"[WS_HANDLER] Training completed for single model: {nameFolder}")
            await websocket.send_json({"status": "training complete"})
            await websocket.send_json(jsonable_encoder(row))
        elif len(row) > 1:
            # print(row)
            logging.info(f"[WS_HANDLER] Processing multiple models ({len(row)} models)")
            split_and_copy_images(row[0])
            await websocket.send_json({"status": "dataset prepared"})
            # print(row)
            for i, r in enumerate(row, 1):
                logging.info(f"[WS_HANDLER] Processing model {i}/{len(row)}: {r['modelname']} (Function: {r['functionname']})")
                
                await websocket.send_json({"status": "training started"})
                type_model = "yolo11n-seg.pt"
                # print(r)  
                if r['functionname'] == "Color Check":
                    type_model = "box-detector_seg.pt"
                elif r['functionname'] == "Classification Type":
                    type_model = "yolo11n-seg.pt"
                elif r['functionname'] == "yolov8m.pt":
                    type_model = "yolov8m.pt"
                elif r['functionname'] == "yolov8l.pt":
                    type_model = "yolov8l.pt"
                else:
                    type_model = "yolov8n.pt"
                    
                logging.info(f"[WS_HANDLER] Model {i}: Selected model type: {type_model}")
                
                nameFolder = f"model_{r['modelversionid']}_{r['functionid']}"
                logging.info(f"[WS_HANDLER] Model {i}: Starting training for {nameFolder}")
                
                await start_training(r, nameFolder, db, websocket , type_model , idFunction=r['functionid'])
                
                logging.info(f"[WS_HANDLER] Model {i}: Training completed for {nameFolder}")
                await websocket.send_json({"status": "completed"})
                
            stop_event.set()
            await keepalive_task
            logging.info(f"[WS_HANDLER] All {len(row)} models completed successfully")
            await websocket.send_json({"status": "completed all"})
             
    except WebSocketDisconnect:
        logging.info(f"[WS_HANDLER] WebSocket disconnected for model {model_id}")
    except Exception as e:
        logging.error(f"[WS_HANDLER] Unexpected error during training action for model {model_id}: {str(e)}")
        await websocket.send_json({"error": str(e)})
        logging.exception(f"[WS_HANDLER] Full exception details for model {model_id}")
    finally:
        # ปิด keepalive task อย่างปลอดภัย
        if keepalive_task and not keepalive_task.done():
            stop_event.set()
            try:
                await asyncio.wait_for(keepalive_task, timeout=2.0)
            except asyncio.TimeoutError:
                keepalive_task.cancel()
        
        # Cleanup websocket connections
        logging.info(f"[WS_HANDLER] Cleaning up WebSocket connection for model {model_id}")
        if model_id in active_websockets and websocket in active_websockets[model_id]:
            active_websockets[model_id].remove(websocket)
            if not active_websockets[model_id]:
                del active_websockets[model_id]
                logging.info(f"[WS_HANDLER] Removed model {model_id} from active websockets")


async def cancel_training_by_modelversion(modelversionid: int, db: Session):
    
    """
    Cancel all active training processes for a specific model version
    
    Args:
        modelversionid (int): The model version ID to cancel training for
        db (Session): Database session
        
    Returns:
        dict: Status of the cancellation operation
    """
    try:
        logging.info(f"[CANCEL_TRAINING] Starting cancellation for model version ID: {modelversionid}")
        
        # Find all PIDs for this model version using the new PID tracking system
        pids_to_cancel = get_pids_by_modelversion(modelversionid)
        
        if not pids_to_cancel:
            logging.info(f"[CANCEL_TRAINING] No active training processes found for model version ID: {modelversionid}")
            
            # Update database status to "Cancelled" anyway
            try:
                sql_update = text("""
                    UPDATE modelversion 
                    SET modelstatus = 'Processing', currentstep = 2 , 
                                modelpath = NULL
                    WHERE modelversionid = :modelversionid AND modelstatus = 'Training'
                """)
                result = db.execute(sql_update, {"modelversionid": modelversionid})
                db.commit()
                
                if result.rowcount > 0:
                    logging.info(f"[CANCEL_TRAINING] Updated database status to 'Cancelled' for model version ID: {modelversionid}")
                    return {
                        "status": "cancelled",
                        "message": f"Model version {modelversionid} marked as cancelled (no active processes found)",
                        "cancelled_processes": 0
                    }
                else:
                    return {
                        "status": "not_found",
                        "message": f"No training model version found with ID {modelversionid}",
                        "cancelled_processes": 0
                    }
                    
            except Exception as db_error:
                logging.error(f"[CANCEL_TRAINING] Database update failed: {str(db_error)}")
                try:
                    db.rollback()
                except:
                    pass
                return {
                    "status": "error",
                    "message": f"Failed to update database: {str(db_error)}",
                    "cancelled_processes": 0
                }
        
        cancelled_count = 0
        failed_cancellations = []
        
        logging.info(f"[CANCEL_TRAINING] Found {len(pids_to_cancel)} PIDs to cancel: {pids_to_cancel}")
        
        # Cancel each process by PID
        for pid_info in pids_to_cancel:
            pid = pid_info.get("PID")
            if not pid:
                continue
                
            try:
                # Use psutil to find and terminate the process
                import psutil
                try:
                    process = psutil.Process(pid)
                    if process.is_running():
                        logging.info(f"[CANCEL_TRAINING] Terminating PID {pid} for model version {modelversionid}")
                        
                        # Try graceful termination first
                        process.terminate()
                        
                        # Wait for graceful termination (3 seconds timeout)
                        try:
                            process.wait(timeout=3)
                            logging.info(f"[CANCEL_TRAINING] PID {pid} terminated gracefully")
                        except psutil.TimeoutExpired:
                            # Force kill if graceful termination fails
                            logging.warning(f"[CANCEL_TRAINING] Graceful termination failed for PID {pid}, force killing...")
                            process.kill()
                            try:
                                process.wait(timeout=5)
                                logging.info(f"[CANCEL_TRAINING] PID {pid} force killed")
                            except psutil.TimeoutExpired:
                                logging.error(f"[CANCEL_TRAINING] Failed to kill PID {pid}")
                                failed_cancellations.append(pid)
                                continue
                        
                        cancelled_count += 1
                    else:
                        logging.info(f"[CANCEL_TRAINING] PID {pid} already finished")
                        
                except psutil.NoSuchProcess:
                    logging.info(f"[CANCEL_TRAINING] PID {pid} no longer exists")
                
                # Remove PID from tracking
                remove_training_pid(modelversionid, pid)
                    
            except ImportError:
                # Fallback to subprocess method if psutil is not available
                logging.warning(f"[CANCEL_TRAINING] psutil not available, using fallback method for PID {pid}")
                try:
                    import signal
                    if sys.platform == "win32":
                        # Windows
                        subprocess.run(["taskkill", "/F", "/PID", str(pid)], check=True)
                    else:
                        # Unix/Linux
                        os.kill(pid, signal.SIGTERM)
                        # Wait a bit then force kill if needed
                        import time
                        time.sleep(3)
                        try:
                            os.kill(pid, 0)  # Check if process still exists
                            os.kill(pid, signal.SIGKILL)  # Force kill
                        except ProcessLookupError:
                            pass  # Process already terminated
                    
                    cancelled_count += 1
                    remove_training_pid(modelversionid, pid)
                    logging.info(f"[CANCEL_TRAINING] PID {pid} terminated using fallback method")
                    
                except Exception as fallback_error:
                    logging.error(f"[CANCEL_TRAINING] Fallback method failed for PID {pid}: {str(fallback_error)}")
                    failed_cancellations.append(pid)
            
            except Exception as cancel_error:
                logging.error(f"[CANCEL_TRAINING] Error cancelling PID {pid}: {str(cancel_error)}")
                failed_cancellations.append(pid)
        
        # Also clean up the old active_training_processes dictionary for this model version
        processes_to_remove = []
        for process_key, process in list(active_training_processes.items()):
            if str(modelversionid) in process_key:
                processes_to_remove.append(process_key)
        
        for process_key in processes_to_remove:
            if process_key in active_training_processes:
                del active_training_processes[process_key]
                logging.info(f"[CANCEL_TRAINING] Removed process {process_key} from active tracking")
        
        # Update database status
        try:
            sql_update = text("""
                UPDATE modelversion 
                SET modelstatus = 'Processing' ,
                 currentstep = 2 , 
                    modelpath = NULL
                WHERE modelversionid = :modelversionid
            """)
            result = db.execute(sql_update, {"modelversionid": modelversionid})
            db.commit()
            
            if result.rowcount > 0:
                logging.info(f"[CANCEL_TRAINING] Updated database status to 'Cancelled' for model version ID: {modelversionid}")
            else:
                logging.warning(f"[CANCEL_TRAINING] No rows updated in database for model version ID: {modelversionid}")
                
        except Exception as db_error:
            logging.error(f"[CANCEL_TRAINING] Database update failed: {str(db_error)}")
            try:
                db.rollback()
            except:
                pass
            return {
                "status": "partial_success",
                "message": f"Processes cancelled but database update failed: {str(db_error)}",
                "cancelled_processes": cancelled_count,
                "failed_cancellations": failed_cancellations
            }
        
        # Close any active websockets for this model version
        model_id_str = str(modelversionid)
        if model_id_str in active_websockets:
            websockets_to_close = active_websockets[model_id_str].copy()
            for websocket in websockets_to_close:
                try:
                    await websocket.send_json({
                        "status": "cancelled",
                        "message": f"Training cancelled for model version {modelversionid}"
                    })
                    await websocket.close()
                    logging.info(f"[CANCEL_TRAINING] Closed websocket for model version ID: {modelversionid}")
                except Exception as ws_error:
                    logging.error(f"[CANCEL_TRAINING] Error closing websocket: {str(ws_error)}")
            
            # Clean up websocket tracking
            active_websockets[model_id_str] = []
        
        # Prepare response
        if failed_cancellations:
            return {
                "status": "partial_success",
                "message": f"Training cancellation completed with some failures for model version {modelversionid}",
                "cancelled_processes": cancelled_count,
                "failed_cancellations": failed_cancellations
            }
        else:
            return {
                "status": "success",
                "message": f"Training successfully cancelled for model version {modelversionid}",
                "cancelled_processes": cancelled_count
            }
            
    except Exception as e:
        logging.exception(f"[CANCEL_TRAINING] Unexpected error during cancellation for model version {modelversionid}: {str(e)}")
        return {
            "status": "error",
            "message": f"Unexpected error during cancellation: {str(e)}",
            "cancelled_processes": 0
        }


def get_active_training_processes():
    """
    Get information about all currently active training processes
    
    Returns:
        dict: Dictionary containing active process information
    """
    active_info = {}
    
    # Get information from the old process tracking system
    for process_key, process in active_training_processes.items():
        try:
            # Check if process is still running
            if process.poll() is None:
                active_info[process_key] = {
                    "pid": process.pid,
                    "status": "running",
                    "tracking_method": "process_object"
                }
            else:
                active_info[process_key] = {
                    "pid": process.pid,
                    "status": "finished",
                    "return_code": process.returncode,
                    "tracking_method": "process_object"
                }
        except Exception as e:
            active_info[process_key] = {
                "pid": "unknown",
                "status": "error",
                "error": str(e),
                "tracking_method": "process_object"
            }
    
    # Get information from the new PID tracking system
    global training_pids
    for pid_info in training_pids:
        modelversionid = pid_info.get("modelversionid")
        pid = pid_info.get("PID")
        key = f"model_version_{modelversionid}_pid_{pid}"
        
        try:
            # Try to check if process is still running using psutil
            try:
                import psutil
                process = psutil.Process(pid)
                if process.is_running():
                    active_info[key] = {
                        "pid": pid,
                        "modelversionid": modelversionid,
                        "status": "running",
                        "tracking_method": "pid_tracking"
                    }
                else:
                    active_info[key] = {
                        "pid": pid,
                        "modelversionid": modelversionid,
                        "status": "finished",
                        "tracking_method": "pid_tracking"
                    }
            except ImportError:
                # Fallback if psutil is not available
                active_info[key] = {
                    "pid": pid,
                    "modelversionid": modelversionid,
                    "status": "unknown",
                    "tracking_method": "pid_tracking",
                    "note": "psutil not available for status check"
                }
            except psutil.NoSuchProcess:
                active_info[key] = {
                    "pid": pid,
                    "modelversionid": modelversionid,
                    "status": "finished",
                    "tracking_method": "pid_tracking"
                }
        except Exception as e:
            active_info[key] = {
                "pid": pid,
                "modelversionid": modelversionid,
                "status": "error",
                "error": str(e),
                "tracking_method": "pid_tracking"
            }
    
    return {
        "active_count": len([info for info in active_info.values() if info.get("status") == "running"]),
        "total_tracked": len(active_info),
        "processes": active_info,
        "pid_tracking_list": training_pids
    }

def get_training_pids_info():
    """
    Get current PID tracking information in the desired JSON format
    
    Returns:
        list: List of PID information in format [{"modelversionid": "16", "PID": 57}, ...]
    """
    global training_pids
    return training_pids.copy()

def cleanup_finished_pids():
    """
    Clean up PID tracking for processes that are no longer running
    """
    global training_pids
    active_pids = []
    
    for pid_info in training_pids:
        pid = pid_info.get("PID")
        if not pid:
            continue
            
        try:
            import psutil
            process = psutil.Process(pid)
            if process.is_running():
                active_pids.append(pid_info)
            else:
                logging.info(f"[PID_CLEANUP] Removing finished PID {pid} for model version {pid_info.get('modelversionid')}")
        except ImportError:
            # Keep all PIDs if psutil is not available
            active_pids.append(pid_info)
        except psutil.NoSuchProcess:
            logging.info(f"[PID_CLEANUP] Removing non-existent PID {pid} for model version {pid_info.get('modelversionid')}")
    
    if len(active_pids) != len(training_pids):
        training_pids = active_pids
        logging.info(f"[PID_CLEANUP] Cleaned up PIDs, now tracking {len(training_pids)} active PIDs")
    
    return len(training_pids)
    